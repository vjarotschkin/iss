# 03_enhanced_panel_creation.R
# Purpose: Create a comprehensive panel dataset for quasi-experimental analysis

# Load necessary packages ----------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  here,       # Path management
  tidyverse,  # Data manipulation
  lubridate,  # Date handling
  zoo,        # Rolling functions
  data.table, # Efficient data operations
  readxl,     # Excel reading
  janitor,    # Clean column names
  logger,     # Better logging
  broom       # Tidy model output
)

# Setup environment ---------------------------------------------------------
# Create necessary directories
dir.create(here::here("data", "checkpoints"), recursive = TRUE, showWarnings = FALSE)
dir.create(here::here("data", "final"), recursive = TRUE, showWarnings = FALSE)
dir.create(here::here("logs"), showWarnings = FALSE)

# Configure logging
log_file <- here::here("logs", paste0("panel_creation_", format(Sys.time(), "%Y%m%d_%H%M"), ".log"))
log_appender(appender_file(log_file))
log_threshold(INFO)

log_info("Starting panel dataset creation process")

# Helper functions ----------------------------------------------------------

#' Check if data has the expected structure
#'
#' @param data Data to check
#' @param purpose Purpose of the data (for log messages)
#' @return TRUE if data is valid, otherwise stops with error
check_data_structure <- function(data, purpose = "") {
  if (is.null(data)) {
    stop(paste("Data for", purpose, "is NULL"))
  }
  
  if (!is.data.frame(data) && !is.data.table(data)) {
    stop(paste("Data for", purpose, "is not a data frame or data table"))
  }
  
  log_info("Data for %s has %d rows and %d columns",
           purpose, nrow(data), ncol(data))
  
  return(TRUE)
}

#' Find a column match from a list of possibilities
#'
#' @param df DataFrame to search in
#' @param preferred_col Preferred column name
#' @param alternative_cols Vector of alternative column names
#' @param log_prefix Prefix for log messages
#' @return Found column name or NULL if no match
find_column_match <- function(df, preferred_col, alternative_cols = c(), log_prefix = "") {
  if (preferred_col %in% names(df)) {
    return(preferred_col)
  }
  
  existing_cols <- intersect(alternative_cols, names(df))
  if (length(existing_cols) > 0) {
    log_info("%sUsing '%s' instead of '%s'", log_prefix, existing_cols[1], preferred_col)
    return(existing_cols[1])
  }
  
  log_warn("%sNeither '%s' nor alternatives %s found. Available columns: %s",
           log_prefix, preferred_col,
           paste(alternative_cols, collapse=", "),
           paste(head(names(df), 20), collapse=", "))
  return(NULL)
}

#' Read a RDS file safely with informative messages
#'
#' @param path Path to the RDS file
#' @return The read object or NULL if file not found
safe_readrds <- function(path) {
  if (file.exists(path)) {
    log_info("Reading %s", basename(path))
    data <- readRDS(path)
    log_info("Loaded %s: %d rows × %d columns",
             basename(path), nrow(data), ncol(data))
    return(data)
  } else {
    log_warn("File not found: %s", path)
    return(NULL)
  }
}

#' Save a checkpoint during the panel creation process
#'
#' @param data The data to save
#' @param step_name Name of the checkpoint step
#' @return The input data invisibly
save_checkpoint <- function(data, step_name) {
  file_path <- here::here("data", "checkpoints", paste0(step_name, ".rds"))
  saveRDS(data, file_path)
  log_info("Saved checkpoint: %s", step_name)
  invisible(data)
}

#' Load a previously saved checkpoint if it exists
#'
#' @param step_name Name of the checkpoint to load
#' @return The loaded data or NULL if checkpoint doesn't exist
load_checkpoint <- function(step_name) {
  file_path <- here::here("data", "checkpoints", paste0(step_name, ".rds"))
  if (file.exists(file_path)) {
    log_info("Loading checkpoint: %s", step_name)
    readRDS(file_path)
  } else {
    log_info("No checkpoint found for: %s", step_name)
    NULL
  }
}

#' Get sample values from a dataframe column
#'
#' @param df Dataframe to sample from
#' @param col_name Column name to sample
#' @param n Number of values to sample
#' @return String with sample values
sample_values <- function(df, col_name, n = 5) {
  if (!col_name %in% names(df)) {
    return("column not found")
  }
  
  vals <- df[[col_name]]
  if (all(is.na(vals))) {
    return("all values are NA")
  }
  
  sample_vals <- unique(na.omit(head(vals, 100)))
  if (length(sample_vals) > n) {
    sample_vals <- sample(sample_vals, n)
  }
  
  paste(as.character(sample_vals), collapse = ", ")
}

# Load and prepare data sources ---------------------------------------------

#' Load all processed datasets needed for panel creation
#'
#' @return A list of all loaded datasets
load_processed_data <- function() {
  processed_dir <- here::here("data", "processed")
  
  # List of required datasets with their corresponding filenames
  required_datasets <- list(
    "opps_data" = "opps_data_cleaned.rds",
    "quotes_data" = "quotes_data_cleaned.rds",
    "sap_software" = "sap_software_cleaned.rds",
    "sap_machines" = "sap_machines_cleaned.rds",
    "sis_installed_base" = "sis_installed_base_cleaned.rds",
    "sis_cases" = "sis_cases_cleaned.rds",
    "sis_missions" = "sis_missions_cleaned.rds",
    "customer_data" = "customer_data_cleaned.rds",
    "contacts_data" = "contacts_data_cleaned.rds",
    "meetings_data" = "meetings_data_cleaned.rds",
    "mails_data" = "mails_data_cleaned.rds",
    "other_act_data" = "other_act_data_cleaned.rds",
    "reg_products" = "registered_products_cleaned.rds"
  )
  
  # Load each dataset
  datasets <- list()
  for (name in names(required_datasets)) {
    file_path <- file.path(processed_dir, required_datasets[[name]])
    datasets[[name]] <- safe_readrds(file_path)
    
    if (!is.null(datasets[[name]])) {
      # Print some sample values for debugging
      if (name %in% c("opps_data", "customer_data", "other_act_data")) {
        id_cols <- grep("id$|customer", names(datasets[[name]]), ignore.case = TRUE, value = TRUE)
        for (col in head(id_cols, 3)) {
          log_info("Sample %s from %s: %s", col, name, sample_values(datasets[[name]], col))
        }
      }
      
      # Standardize ID columns as character
      id_cols <- grep("^customer.*id$|^to_customer_id$|^account_id$|^sap.id$",
                      names(datasets[[name]]), ignore.case = TRUE, value = TRUE)
      
      for (col in id_cols) {
        if (col %in% names(datasets[[name]])) {
          datasets[[name]][[col]] <- as.character(datasets[[name]][[col]])
        }
      }
    }
  }
  
  return(datasets)
}

#' Load product category definitions
#'
#' @return A list of vectors containing product categories
load_product_categories <- function() {
  # Path to the product categories file
  file_path <- here::here("reports", "Tables",
                          "210517_product_categories_c4c_own_categorization_Anm._Felix_tables_vaj.xlsx")
  
  if (!file.exists(file_path)) {
    log_warn("Product categories file not found at %s", file_path)
    log_info("Using hardcoded product categories as fallback")
    
    return(list(
      t_accessories = c("Accessories", "Accessory", "Zubehör"),
      t_hw_mt_standalone = c("Punching machine", "Press brake"),
      t_hw_lt_standalone = c("Laser cutting machine", "Laser machine"),
      t_mt_system_solution = c("System solution", "Systemlösung"),
      t_other_miss = c("Other"),
      t_services = c("Service", "Services", "Dienstleistung"),
      t_software = c("Software", "TruTops"),
      t_spare_parts = c("Spare part", "Spare parts", "Ersatzteile"),
      t_unclear = c(),
      exploration_pattern = paste(
        "Quickjob", "Customer", "Monitoring", "Calculate",
        "Monitor", "Calculation\\(CaaS\\)", "Webcalculation",
        "Purchasing", "TruTopsMonitor", "Production",
        "Sonstige Fabrication SW", "Cell", "Equipment Manager",
        sep = "|"
      ),
      exploitation_pattern = paste(
        "BOOST", "Weld", "Classic", "Tube", "Bend",
        sep = "|"
      )
    ))
  }
  
  # Sheet mapping: names with their corresponding sheet numbers
  sheet_map <- list(
    t_accessories = 2,
    t_hw_mt_standalone = 3,
    t_hw_lt_standalone = 4,
    t_mt_system_solution = 5,
    t_other_miss = 6,
    t_services = 7,
    t_software = 8,
    t_spare_parts = 9,
    t_unclear = 10
  )
  
  # Read each sheet and extract product categories
  categories <- list()
  for (name in names(sheet_map)) {
    sheet_num <- sheet_map[[name]]
    
    tryCatch({
      categories[[name]] <- readxl::read_excel(file_path, sheet = sheet_num) %>%
        dplyr::distinct(Produktkategorie) %>%
        dplyr::pull(Produktkategorie)
      
      log_info("Loaded %s categories: %d items", name, length(categories[[name]]))
    }, error = function(e) {
      log_error("Failed to load %s categories: %s", name, e$message)
      categories[[name]] <- character(0)  # Set empty vector as fallback
    })
  }
  
  # Define software pattern definitions for exploration and exploitation
  categories$exploration_pattern <- paste(
    "Quickjob", "Customer", "Monitoring", "Calculate",
    "Monitor", "Calculation\\(CaaS\\)", "Webcalculation",
    "Purchasing", "TruTopsMonitor", "Production",
    "Sonstige Fabrication SW", "Cell", "Equipment Manager",
    sep = "|"
  )
  
  categories$exploitation_pattern <- paste(
    "BOOST", "Weld", "Classic", "Tube", "Bend",
    sep = "|"
  )
  
  return(categories)
}

# Panel creation functions --------------------------------------------------

#' Create the base panel structure with customer-year combinations
#'
#' @param opps_data Opportunities data
#' @param year_range Range of years to include
#' @return A dataframe with customer-year combinations and relationship status
create_base_panel <- function(opps_data, year_range = c(1990, 2020)) {
  log_info("Creating base panel structure for years %d-%d", year_range[1], year_range[2])
  
  # Verify data structure
  check_data_structure(opps_data, "opportunities for base panel")
  
  # Check if required columns exist
  required_cols <- c("to_customer_id", "to_start_year")
  missing_cols <- setdiff(required_cols, names(opps_data))
  
  if (length(missing_cols) > 0) {
    log_info("Missing required columns: %s", paste(missing_cols, collapse=", "))
    
    # Log available columns for debugging
    log_info("Available columns: %s", paste(head(names(opps_data), 30), collapse=", "))
    
    # Try to derive to_start_year if it's missing but to_start_date exists
    if ("to_start_year" %in% missing_cols && "to_start_date" %in% names(opps_data)) {
      log_info("Attempting to derive to_start_year from to_start_date")
      opps_data <- opps_data %>%
        dplyr::mutate(to_start_year = lubridate::year(to_start_date))
      
      # Check if derivation worked
      if ("to_start_year" %in% names(opps_data)) {
        missing_cols <- setdiff(required_cols, names(opps_data))
        log_info("Successfully derived to_start_year")
        log_info("Sample to_start_year values: %s",
                 paste(head(na.omit(opps_data$to_start_year)), collapse=", "))
      }
    }
    
    # For to_customer_id, try alternative columns
    if ("to_customer_id" %in% missing_cols) {
      customer_id_col <- find_column_match(
        opps_data,
        "to_customer_id",
        c("customer_id", "account_id", "id_customer", "customer")
      )
      
      if (!is.null(customer_id_col)) {
        opps_data <- opps_data %>%
          dplyr::mutate(to_customer_id = .data[[customer_id_col]])
        missing_cols <- setdiff(required_cols, names(opps_data))
      }
    }
    
    # If still missing critical columns, abort
    if (length(missing_cols) > 0) {
      stop(paste("Cannot create base panel: missing columns", paste(missing_cols, collapse=", ")))
    }
  }
  
  # Extract first interaction year for each customer
  first_interactions <- opps_data %>%
    dplyr::mutate(to_customer_id = as.character(to_customer_id)) %>%
    dplyr::group_by(to_customer_id) %>%
    dplyr::summarise(
      first_interaction = min(to_start_year, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    # Filter out invalid years (NA or unreasonably old/recent)
    dplyr::filter(!is.na(first_interaction) &
                    first_interaction >= year_range[1] - 10 &
                    first_interaction <= year_range[2] + 5)
  
  log_info("Found %d customers with valid first interaction years",
           nrow(first_interactions))
  
  # Create the panel structure with all customer-year combinations
  panel_base <- tidyr::expand_grid(
    customer_id = unique(first_interactions$to_customer_id),
    year = seq(year_range[1], year_range[2])
  ) %>%
    dplyr::left_join(
      first_interactions %>% dplyr::rename(customer_id = to_customer_id),
      by = "customer_id"
    ) %>%
    dplyr::mutate(
      relationship_status = dplyr::case_when(
        year < first_interaction ~ "pre_relationship",
        year >= first_interaction ~ "active",
        TRUE ~ NA_character_
      )
    )
  
  log_info("Base panel created: %d customers × %d years = %d rows",
           n_distinct(panel_base$customer_id),
           n_distinct(panel_base$year),
           nrow(panel_base))
  
  return(panel_base)
}

#' Add customer characteristics to the panel
#'
#' @param panel_data Base panel data
#' @param opps_data Opportunities data
#' @param customer_data Customer data
#' @return Panel with customer characteristics added
add_customer_characteristics <- function(panel_data, opps_data, customer_data) {
  log_info("Adding customer characteristics to the panel")
  
  # --- 1. Data Validation and Preparation ---
  
  # Check input data structures
  check_data_structure(panel_data, "panel data for customer characteristics")
  check_data_structure(opps_data, "opportunities data for customer characteristics")
  
  # Check customer_data (if provided)
  if (!is.null(customer_data)) {
    check_data_structure(customer_data, "customer data for customer characteristics")
    
    # Standardize customer ID column in customer_data (if it exists)
    customer_id_col_cust <- find_column_match(
      customer_data,
      "customer_id",
      c("id_customer", "account_id", "to_customer_id", "customer") #possible alternatives
    )
    if (!is.null(customer_id_col_cust)) {
      customer_data <- customer_data %>%
        dplyr::rename(customer_id = all_of(customer_id_col_cust)) %>%
        dplyr::mutate(customer_id = as.character(customer_id))
      log_info("customer_data: Renamed '%s' to 'customer_id' and converted to character", customer_id_col_cust)
    } else {
      log_warn("customer_data: No suitable 'customer_id' column found.")
    }
    
  } else {
    log_warn("Customer data is NULL. Skipping direct customer characteristics.")
  }
  
  # Standardize customer ID column in opps_data
  customer_id_col_opps <- find_column_match(
    opps_data,
    "to_customer_id",
    c("customer_id", "account_id", "id_customer", "customer") #possible alternatives
  )
  
  if (!is.null(customer_id_col_opps)) {
    opps_data <- opps_data %>%
      dplyr::rename(customer_id = all_of(customer_id_col_opps)) %>%
      dplyr::mutate(customer_id = as.character(customer_id))
    log_info("opps_data: Renamed '%s' to 'customer_id' and converted to character.", customer_id_col_opps)
  } else {
    log_error("opps_data: No suitable 'customer_id' column found. Aborting.")
    return(panel_data) # Return original panel if crucial ID is missing
  }
  
  # --- 2. Feature Engineering from Opportunities Data ---
  
  # First interaction year (already calculated in base panel, but ensure it's present)
  first_interaction <- opps_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::summarise(first_interaction_year = min(lubridate::year(to_start_date), na.rm = TRUE),
                     .groups = 'drop')
  
  log_info("Calculated first interaction year for %d customers", nrow(first_interaction))
  
  # --- 3. Feature Engineering from Customer Data (if available)---
  
  if (!is.null(customer_data)) {
    # Extract relevant customer characteristics (e.g., industry, size)
    customer_info <- customer_data %>%
      dplyr::select(
        customer_id,
        industry = classification,  # Assuming 'classification' is industry
        account_size = numberofemployees # Example, adjust column names as needed
      ) %>%
      dplyr::distinct(customer_id, .keep_all = TRUE)  # Ensure one row per customer
    
    log_info("Extracted customer info (industry, size) for %d customers", nrow(customer_info))
    
    # --- 4. Joining Data ---
    
    # Join first interaction year
    panel_data <- panel_data %>%
      dplyr::left_join(first_interaction, by = "customer_id")
    
    # Join customer information
    panel_data <- panel_data %>%
      dplyr::left_join(customer_info, by = "customer_id")
    
  } else {
    # If customer_data is not available, only join first interaction
    panel_data <- panel_data %>%
      dplyr::left_join(first_interaction, by = "customer_id")
  }
  
  # --- 5. Create Time-Varying Variables ---
  # (If customer info is available and we have time-varying customer attributes)
  
  
  # --- 6. Finalize and Return ---
  
  log_info("Customer characteristics added. Panel size: %d rows", nrow(panel_data))
  return(panel_data)
}



#' Add interaction counts to the panel
#'
#' @param panel_data Panel data
#' @param opps_data Opportunities data
#' @param quotes_data Quotes data
#' @param meetings_data Meetings data
#' @param mails_data Mails data
#' @param other_act_data other activities data
#' @return Panel with interaction counts added
add_interaction_counts <- function(panel_data, opps_data, quotes_data,
                                   meetings_data, mails_data, other_act_data) {
  
  log_info("Adding interaction counts to the panel")
  
  # --- 1. Data Validation and Preparation ---
  check_data_structure(panel_data, "panel data for interaction counts")
  
  # Function to standardize and prepare interaction data
  prepare_interaction_data <- function(data, data_name, id_col, date_col) {
    if (is.null(data)) {
      log_info("%s data is NULL. Skipping.", data_name)
      return(NULL)
    }
    check_data_structure(data, paste(data_name, "for interaction counts"))
    
    # Find and standardize ID column
    found_id_col <- find_column_match(data, id_col,
                                      c("customer_id", "account_id", "to_customer_id", "customer"), #common alternatives
                                      log_prefix = paste0(data_name, ": "))
    
    if (is.null(found_id_col)) {
      log_warn("%s: No suitable customer ID column. Skipping.", data_name)
      return(NULL)
    }
    
    # Find and standardize Date column
    found_date_col <- find_column_match(data, date_col,
                                        c("date", "created_at", "start_date", "close_date"), #common alternatives
                                        log_prefix = paste0(data_name, ": "))
    if (is.null(found_date_col)) {
      log_warn("%s: No suitable date column. Skipping.", data_name)
      return(NULL)
    }
    
    data <- data %>%
      dplyr::rename(customer_id = all_of(found_id_col),
                    interaction_date = all_of(found_date_col)) %>%
      dplyr::mutate(customer_id = as.character(customer_id),
                    interaction_year = lubridate::year(interaction_date)) %>%
      dplyr::select(customer_id, interaction_year)
    
    log_info("%s: Standardized ID and date columns.", data_name)
    return(data)
  }
  
  # Prepare each interaction dataset
  opps_data_prep <- prepare_interaction_data(opps_data, "Opportunities", "to_customer_id", "to_start_date")
  quotes_data_prep <- prepare_interaction_data(quotes_data, "Quotes", "customer_id", "validfromdate")
  meetings_data_prep <- prepare_interaction_data(meetings_data, "Meetings", "account_id", "start_date_time")
  mails_data_prep <- prepare_interaction_data(mails_data, "Mails", "account_id", "creation_date_time")
  other_act_data_prep <- prepare_interaction_data(other_act_data, "Other Activities", "customer_id", "creationdatetime")
  
  # --- 2. Aggregate Interaction Counts ---
  
  # Function to aggregate counts
  aggregate_interactions <- function(data, data_name) {
    if (is.null(data)) return(NULL)
    
    counts <- data %>%
      dplyr::group_by(customer_id, interaction_year) %>%
      dplyr::summarise(!!paste0("n_", tolower(data_name)) := n(), .groups = "drop")
    log_info("Aggregated %s counts.", data_name)
    return(counts)
  }
  
  # Aggregate counts for each dataset
  opps_counts <- aggregate_interactions(opps_data_prep, "Opportunities")
  quotes_counts <- aggregate_interactions(quotes_data_prep, "Quotes")
  meetings_counts <- aggregate_interactions(meetings_data_prep, "Meetings")
  mails_counts <- aggregate_interactions(mails_data_prep, "Mails")
  other_act_counts <- aggregate_interactions(other_act_data_prep, "Other Activities")
  
  # --- 3. Join Interaction Counts to Panel ---
  
  # Join function
  join_counts <- function(panel_data, counts_data) {
    if (is.null(counts_data)) return(panel_data)
    
    panel_data %>%
      dplyr::left_join(counts_data, by = c("customer_id" = "customer_id", "year" = "interaction_year"))
  }
  
  panel_data <- join_counts(panel_data, opps_counts)
  panel_data <- join_counts(panel_data, quotes_counts)
  panel_data <- join_counts(panel_data, meetings_counts)
  panel_data <- join_counts(panel_data, mails_counts)
  panel_data <- join_counts(panel_data, other_act_counts)
  
  # --- 4. Handle Missing Values ---
  
  # Fill NA counts with 0 (assuming no record means no interaction)
  interaction_cols <- grep("^n_", names(panel_data), value = TRUE)
  panel_data <- panel_data %>%
    dplyr::mutate(across(all_of(interaction_cols), ~replace_na(., 0)))
  
  log_info("Interaction counts added and NAs filled. Panel size: %d rows", nrow(panel_data))
  return(panel_data)
}



#' Add installed base information (number of machines)
#'
#' @param panel_data Panel data
#' @param sis_installed_base Installed base data from SIS
#' @return Panel with installed base information
add_installed_base <- function(panel_data, sis_installed_base) {
  
  log_info("Adding installed base information to the panel")
  
  # --- 1. Data Validation and Preparation ---
  check_data_structure(panel_data, "panel_data for installed base")
  if(is.null(sis_installed_base)) {
    log_warn("sis_installed_base data is NULL.  Returning original panel.")
    return(panel_data)
  }
  check_data_structure(sis_installed_base, "sis_installed_base for installed base")
  
  # Standardize customer ID
  customer_id_col <- find_column_match(
    sis_installed_base,
    "customer_id",
    c("id_customer", "account_id", "to_customer_id", "customer"), #common alternatives
    log_prefix = "sis_installed_base: "
  )
  
  if (is.null(customer_id_col)) {
    log_error("sis_installed_base: No suitable customer ID column found. Returning original panel.")
    return(panel_data)
  }
  
  sis_installed_base <- sis_installed_base %>%
    dplyr::rename(customer_id = all_of(customer_id_col)) %>%
    dplyr::mutate(customer_id = as.character(customer_id))
  
  
  # --- 2. Feature Engineering: Machine Counts ---
  
  # Count machines per customer and year
  machine_counts <- sis_installed_base %>%
    dplyr::group_by(customer_id, year = lubridate::year(delivery_date)) %>%
    dplyr::summarise(n_machines_installed = n_distinct(serial_number), .groups = "drop")  # Count distinct serial numbers
  
  log_info("Calculated yearly machine counts for %d customer-year combinations", nrow(machine_counts))
  
  
  # --- 3. Join to Panel ---
  
  panel_data <- panel_data %>%
    dplyr::left_join(machine_counts, by = c("customer_id", "year"))
  
  # --- 4. Handle Missing Values ---
  
  # Fill NA counts with 0 (assuming no record means no machines installed that year)
  panel_data <- panel_data %>%
    dplyr::mutate(n_machines_installed = replace_na(n_machines_installed, 0))
  
  # --- 5. Cumulative Machine Counts ---
  panel_data <- panel_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      cumulative_machines = cumsum(n_machines_installed)
    ) %>%
    ungroup()
  
  
  log_info("Installed base information added. Panel size: %d rows", nrow(panel_data))
  
  return(panel_data)
}


#' Add registered product information
#'
#' @param panel_data The panel data
#' @param reg_products Registered products data
#' @return Panel data with registered product information added
add_registered_products <- function(panel_data, reg_products) {
  log_info("Adding registered product information to the panel")
  
  # --- 1. Data Validation and Preparation ---
  
  check_data_structure(panel_data, "panel_data for registered products")
  if (is.null(reg_products)) {
    log_warn("reg_products data is NULL. Returning original panel.")
    return(panel_data)
  }
  check_data_structure(reg_products, "reg_products for registered products")
  
  # Standardize and find customer ID column
  customer_id_col <- find_column_match(
    reg_products,
    "customer_id",
    c("id_customer", "account_id", "to_customer_id", "customer", "sap.id"),
    log_prefix = "reg_products: "
  )
  if (is.null(customer_id_col)) {
    log_error("reg_products: No suitable customer ID column. Returning original panel.")
    return(panel_data)
  }
  reg_products <- reg_products %>%
    dplyr::rename(customer_id = all_of(customer_id_col)) %>%
    dplyr::mutate(customer_id = as.character(customer_id))
  
  # Standardize and find product registration date
  date_col <- find_column_match(reg_products, "reg_date", c("creation_date", "date"), log_prefix = "reg_products: ")
  if(is.null(date_col)) {
    log_warn("reg_products: No suitable date column found for registration date.  Using 'changed_on'.")
    date_col <- "changed_on"  # Fallback to 'changed_on' if no primary date
  }
  reg_products <- reg_products %>%
    dplyr::rename(reg_date = all_of(date_col))
  
  
  # --- 2. Feature Engineering ---
  # Aggregate registered product data to get yearly counts
  
  yearly_reg_products <- reg_products %>%
    dplyr::mutate(reg_year = lubridate::year(reg_date)) %>%
    dplyr::group_by(customer_id, reg_year) %>%
    dplyr::summarise(
      n_reg_products = n(),
      .groups = "drop"
    )
  
  log_info("Aggregated registered products by year, resulting in %d rows.", nrow(yearly_reg_products))
  
  # --- 3. Join to Panel ---
  panel_data <- panel_data %>%
    dplyr::left_join(yearly_reg_products, by = c("customer_id" = "customer_id", "year" = "reg_year")) %>%
    dplyr::mutate(n_reg_products = tidyr::replace_na(n_reg_products, 0)) #treat NA as 0
  
  log_info("Registered product information added. Panel size: %d rows", nrow(panel_data))
  
  
  return(panel_data)
}

#' Aggregate and add service information (cases and missions)
#' @param panel_data The current panel data
#' @param sis_cases The SIS cases data
#' @param sis_missions The SIS missions data
#' @return panel_data with the added service information
add_service_info <- function(panel_data, sis_cases, sis_missions) {
  log_info("Adding service information (cases and missions) to the panel")
  
  # --- 1. Data Validation ---
  check_data_structure(panel_data, "panel_data for service info")
  if (!is.null(sis_cases)) {
    check_data_structure(sis_cases, "sis_cases for service info")
  } else {
    log_warn("sis_cases data is NULL. Skipping case information.")
  }
  if (!is.null(sis_missions)) {
    check_data_structure(sis_missions, "sis_missions for service info")
  } else {
    log_warn("sis_missions data is NULL. Skipping mission information.")
  }
  
  # --- 2. Helper Function: Prepare and Aggregate Service Data ---
  
  prepare_and_aggregate_service <- function(data, data_name, id_col, date_col, count_col) {
    if (is.null(data)) {
      log_info("%s data is NULL. Skipping.", data_name)
      return(NULL)
    }
    
    # Standardize customer ID
    found_id_col <- find_column_match(
      data, id_col,
      c("customer_id", "id_customer", "account_id", "to_customer_id", "customer"),
      log_prefix = paste0(data_name, ": ")
    )
    if (is.null(found_id_col)) {
      log_warn("%s: No suitable customer ID column. Skipping.", data_name)
      return(NULL)
    }
    data <- data %>%
      dplyr::rename(customer_id = all_of(found_id_col)) %>%
      dplyr::mutate(customer_id = as.character(customer_id))
    
    
    # Standardize date column and extract year
    found_date_col <- find_column_match(data, date_col, c("date", "creation_date", "start_date"), log_prefix = paste0(data_name, ": "))
    if(is.null(found_date_col)){
      log_warn("%s: No suitable date column. Skipping.", data_name)
      return(NULL)
    }
    
    data <- data %>%
      dplyr::rename(service_date = all_of(found_date_col)) %>%
      dplyr::mutate(service_year = lubridate::year(service_date))
    
    # Aggregate counts
    aggregated_data <- data %>%
      dplyr::group_by(customer_id, service_year) %>%
      dplyr::summarise(!!count_col := n(), .groups = "drop")
    
    log_info("Aggregated %s data.", data_name)
    return(aggregated_data)
  }
  
  # --- 3. Process Cases and Missions ---
  
  cases_aggregated <- prepare_and_aggregate_service(sis_cases, "SIS Cases", "customer_id", "creation_date", "n_cases")
  missions_aggregated <- prepare_and_aggregate_service(sis_missions, "SIS Missions", "customer_id", "date", "n_missions")
  
  
  # --- 4. Join to Panel ---
  
  if (!is.null(cases_aggregated)) {
    panel_data <- panel_data %>%
      dplyr::left_join(cases_aggregated, by = c("customer_id" = "customer_id", "year" = "service_year"))
  }
  if (!is.null(missions_aggregated)) {
    panel_data <- panel_data %>%
      dplyr::left_join(missions_aggregated, by = c("customer_id" = "customer_id", "year" = "service_year"))
  }
  
  
  # --- 5. Handle Missing Values ---
  
  panel_data <- panel_data %>%
    dplyr::mutate(
      n_cases = replace_na(n_cases, 0),
      n_missions = replace_na(n_missions, 0)
    )
  
  log_info("Service information (cases and missions) added. Panel size: %d rows", nrow(panel_data))
  return(panel_data)
}


#' Add software usage information (exploration and exploitation)
#'
#' @param panel_data Panel data
#' @param sap_software SAP software data
#' @param product_categories List of product categories
#' @return Panel data with software usage information
add_software_usage <- function(panel_data, sap_software, product_categories) {
  log_info("Adding software usage information (exploration and exploitation) to the panel.")
  
  # --- 1. Data Validation and Preparation ---
  check_data_structure(panel_data, "panel_data for software usage")
  if (is.null(sap_software)) {
    log_warn("sap_software data is NULL. Returning original panel.")
    return(panel_data)
  }
  check_data_structure(sap_software, "sap_software for software usage")
  
  # Standardize customer ID and rename to customer_id
  customer_id_col_sw <- find_column_match(
    sap_software,
    "customer_id",
    c("id_customer", "account_id", "to_customer_id", "customer", "sap.id"), #common alternatives
    log_prefix = "sap_software: "
  )
  if(is.null(customer_id_col_sw)){
    log_error("sap_software: No suitable customer ID. Returning original panel.")
    return(panel_data)
  }
  sap_software <- sap_software %>%
    dplyr::rename(customer_id = all_of(customer_id_col_sw)) %>%
    dplyr::mutate(customer_id = as.character(customer_id))
  
  # --- 2. Feature Engineering: Exploration and Exploitation ---
  
  # Check for software categories and patterns (with logging)
  if(is.null(product_categories$t_software) || length(product_categories$t_software) == 0) {
    log_warn("No software categories (t_software) found in product_categories.")
  }
  if(is.null(product_categories$exploration_pattern) || nchar(product_categories$exploration_pattern) == 0) {
    log_warn("No exploration pattern found in product_categories.")
  }
  if(is.null(product_categories$exploitation_pattern) || nchar(product_categories$exploitation_pattern) == 0){
    log_warn("No exploitation pattern found in product_categories")
  }
  
  software_usage <- sap_software %>%
    dplyr::mutate(
      usage_year = lubridate::year(valid_from),
      is_exploration = dplyr::case_when(
        product_category %in% product_categories$t_software &
          grepl(product_categories$exploration_pattern, product, ignore.case = TRUE) ~ 1,
        TRUE ~ 0
      ),
      is_exploitation = dplyr::case_when(
        product_category %in% product_categories$t_software &
          grepl(product_categories$exploitation_pattern, product, ignore.case = TRUE) ~ 1,
        TRUE ~ 0
      )
    ) %>%
    dplyr::group_by(customer_id, usage_year) %>%
    dplyr::summarise(
      has_exploration = max(is_exploration, na.rm = TRUE),
      has_exploitation = max(is_exploitation, na.rm = TRUE),
      .groups = "drop"
    )
  
  log_info("Classified software usage (exploration/exploitation) for %d customer-year combinations.", nrow(software_usage))
  
  # --- 3. Join to Panel ---
  
  panel_data <- panel_data %>%
    dplyr::left_join(software_usage, by = c("customer_id" = "customer_id", "year" = "usage_year"))
  
  # --- 4. Handle Missing Values and Convert to Indicator ---
  
  panel_data <- panel_data %>%
    dplyr::mutate(
      has_exploration = tidyr::replace_na(has_exploration, 0),
      has_exploitation = tidyr::replace_na(has_exploitation, 0)
    )
  
  log_info("Software usage information added. Panel size: %d rows", nrow(panel_data))
  return(panel_data)
}


# Main panel assembly ------------------------------------------------------
build_panel <- function() {
  log_info("Starting the panel building process...")
  
  # --- 1. Load Data ---
  datasets <- load_processed_data()
  product_categories <- load_product_categories()
  
  # --- 2. Create Base Panel ---
  # Try to load a checkpoint; if it fails, create the base panel
  panel_data <- load_checkpoint("base_panel")
  if (is.null(panel_data)) {
    panel_data <- create_base_panel(datasets$opps_data)
    save_checkpoint(panel_data, "base_panel")
  }
  
  # --- 3. Add Customer Characteristics ---
  panel_data <- load_checkpoint("customer_characteristics")
  if(is.null(panel_data)){
    panel_data <- add_customer_characteristics(panel_data, datasets$opps_data, datasets$customer_data)
    save_checkpoint(panel_data, "customer_characteristics")
  }
  
  # --- 4. Add Interaction Counts ---
  panel_data <- load_checkpoint("interaction_counts")
  if(is.null(panel_data)){
    panel_data <- add_interaction_counts(panel_data, datasets$opps_data,
                                         datasets$quotes_data, datasets$meetings_data,
                                         datasets$mails_data, datasets$other_act_data)
    save_checkpoint(panel_data, "interaction_counts")
  }
  
  
  # --- 5. Add Installed Base ---
  panel_data <- load_checkpoint("installed_base")
  if(is.null(panel_data)){
    panel_data <- add_installed_base(panel_data, datasets$sis_installed_base)
    save_checkpoint(panel_data, "installed_base")
  }
  
  # --- 6. Add Registered Products ---
  panel_data <- load_checkpoint("registered_products")
  if(is.null(panel_data)){
    panel_data <- add_registered_products(panel_data, datasets$reg_products)
    save_checkpoint(panel_data, "registered_products")
  }
  
  # --- 7. Add Service Info (Cases and Missions) ---
  panel_data <- load_checkpoint("service_info")
  if(is.null(panel_data)){
    panel_data <- add_service_info(panel_data, datasets$sis_cases, datasets$sis_missions)
    save_checkpoint(panel_data, "service_info")
  }
  
  # --- 8. Add Software Usage ---
  panel_data <- load_checkpoint("software_usage")
  if(is.null(panel_data)){
    panel_data <- add_software_usage(panel_data, datasets$sap_software, product_categories)
    save_checkpoint(panel_data, "software_usage")
  }
  
  # --- 9. Finalize and Save ---
  
  # Convert to data.table for efficiency
  panel_data <- data.table::as.data.table(panel_data)
  
  # Save the final panel
  saveRDS(panel_data, here::here("data", "final", "panel_data.rds"))
  log_info("Panel building process completed. Final panel saved.")
  
  return(panel_data)
}

# Run the panel building process -------------------------------------------
panel_data <- build_panel()