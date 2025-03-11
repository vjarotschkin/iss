# 03_enhanced_panel_creation.R
# Purpose: Create a comprehensive panel dataset for quasi-experimental analysis
# of the impact of exploration software purchases on customer relationships

# Load necessary packages ----------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  here,           # Path management
  tidyverse,      # Data manipulation
  lubridate,      # Date handling
  zoo,            # Rolling functions
  data.table,     # Efficient data operations
  readxl,         # Excel reading
  janitor,        # Clean column names
  logger,         # Better logging
  broom,          # Tidy model output
  parallel,       # Parallel processing for large operations
  rlang           # For working with dynamic column names
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
safe_readRDS <- function(path) {
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
    datasets[[name]] <- safe_readRDS(file_path)
    
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
  log_info("Adding customer characteristics")
  
  # First, check what columns are actually available in the data
  log_info("Available columns in opportunities data: %s", 
           paste(names(opps_data), collapse=", "))
  
  # Find customer name column
  customer_name_col <- find_column_match(
    opps_data, 
    "to_customer_name", 
    c("customer_name", "to_name_of_customer", "account", "company")
  )
  
  # Find business model column 
  business_model_col <- find_column_match(
    opps_data, 
    "business_model", 
    c("to_business_model", "business")
  )
  
  # Find main sector column
  main_sector_col <- find_column_match(
    opps_data, 
    "main_sector", 
    c("to_main_sector", "sector", "industry")
  )
  
  # Find classification column
  classification_col <- find_column_match(
    opps_data, 
    "abc_classification", 
    c("to_abc_classification", "classification", "customer_class")
  )
  
  # Find country column
  country_col <- find_column_match(
    opps_data, 
    "country", 
    c("to_country", "country_code", "state")
  )
  
  # Extract customer characteristics - using more flexible column selection
  # that works with the actual column names in the data
  customer_chars <- opps_data %>%
    dplyr::select(to_customer_id, dplyr::all_of(c(
      customer_name_col, business_model_col, main_sector_col, 
      classification_col, country_col
    ))) %>%
    # Standardize column names if they exist
    dplyr::rename_with(
      ~ case_when(
        . == customer_name_col ~ "customer_name",
        . == business_model_col ~ "business_model",
        . == main_sector_col ~ "main_sector",
        . == classification_col ~ "abc_classification",
        . == country_col ~ "country",
        TRUE ~ .
      )
    ) %>%
    dplyr::distinct(to_customer_id, .keep_all = TRUE) %>%
    dplyr::mutate(to_customer_id = as.character(to_customer_id))
  
  log_info("Extracted customer characteristics with columns: %s", 
           paste(names(customer_chars), collapse=", "))
  
  # Join with customer data if available
  if (!is.null(customer_data)) {
    # Check if customer_data has required columns
    customer_id_col <- find_column_match(
      customer_data, 
      "customer_account_id", 
      c("account_id", "customer_id", "id")
    )
    
    customer_sap_id_col <- find_column_match(
      customer_data, 
      "customer_sap_id", 
      c("sap_id", "erp_id")
    )
    
    if (!is.null(customer_id_col) && !is.null(customer_sap_id_col)) {
      customer_chars <- customer_chars %>%
        dplyr::left_join(
          customer_data %>% 
            dplyr::select(dplyr::all_of(c(customer_id_col, customer_sap_id_col))) %>%
            dplyr::rename(
              customer_account_id = !!customer_id_col,
              customer_sap_id = !!customer_sap_id_col
            ) %>%
            dplyr::mutate(customer_account_id = as.character(customer_account_id)),
          by = c("to_customer_id" = "customer_account_id")
        )
    }
  }
  
  # Add to panel data
  panel_data <- panel_data %>%
    dplyr::left_join(
      customer_chars,
      by = c("customer_id" = "to_customer_id")
    ) %>%
    dplyr::group_by(customer_id) %>%
    # Fill customer characteristics across all years
    tidyr::fill(colnames(customer_chars)[-1], .direction = "downup") %>%
    dplyr::ungroup()
  
  log_info("Added customer characteristics")
  return(panel_data)
}

#' Add opportunity-related features to the panel
#'
#' @param panel_data Panel data
#' @param opps_data Opportunities data
#' @param product_categories Product category definitions
#' @return Panel with opportunity features added
# add_opportunity_features <- function(panel_data, opps_data, product_categories) {
#   log_info("Adding opportunity features")
#   
#   # Check data structure
#   check_data_structure(opps_data, "opportunities data")
#   check_data_structure(panel_data, "panel data")
#   
#   # Find or derive required columns for opportunities
#   status_col <- find_column_match(
#     opps_data, 
#     "to_opportunity_status", 
#     c("opportunity_status", "status", "state")
#   )
#   
#   product_cat_col <- find_column_match(
#     opps_data, 
#     "to_product_category", 
#     c("product_category", "category", "product_type")
#   )
#   
#   value_col <- find_column_match(
#     opps_data, 
#     "to_expected_value", 
#     c("expected_value", "value", "amount")
#   )
#   
#   close_year_col <- find_column_match(
#     opps_data, 
#     "to_close_year", 
#     c("close_year", "year", "opp_year")
#   )
#   
#   # If close_year is missing but close_date exists, derive it
#   if (is.null(close_year_col) && "to_close_date" %in% names(opps_data)) {
#     log_info("Deriving to_close_year from to_close_date")
#     opps_data <- opps_data %>%
#       dplyr::mutate(to_close_year = lubridate::year(to_close_date))
#     close_year_col <- "to_close_year"
#   }
#   
#   # Log required columns for reporting
#   required_cols <- c(
#     "to_customer_id", status_col, product_cat_col, 
#     value_col, close_year_col
#   )
#   
#   log_info("Using columns for opportunity features: %s", 
#            paste(required_cols, collapse=", "))
#   
#   # Check if we have the required columns
#   if (any(sapply(required_cols, is.null))) {
#     missing_cols <- required_cols[sapply(required_cols, is.null)]
#     stop(paste("Missing required columns for opportunity features:", 
#                paste(missing_cols, collapse=", ")))
#   }
#   
#   # Add debug info for exploration pattern
#   log_info("Checking for exploration patterns in column: %s", product_cat_col)
#   log_info("Using exploration pattern: %s", product_categories$exploration_pattern)
#   log_info("Sample product categories: %s", 
#            paste(head(unique(opps_data[[product_cat_col]]), 10), collapse=", "))
#   
#   # Add exploration/exploitation flags to opportunities
#   opps_with_flags <- opps_data %>%
#     dplyr::mutate(
#       to_customer_id = as.character(to_customer_id),
#       
#       # Add exploration software purchase flag
#       exploration_sw_purchase = dplyr::case_when(
#         grepl(product_categories$exploration_pattern, .data[[product_cat_col]], ignore.case = TRUE) &
#           .data[[status_col]] == "Won" ~ 1,
#         TRUE ~ 0
#       ),
#       
#       # Add exploitation software purchase flag
#       exploitation_sw_purchase = dplyr::case_when(
#         grepl(product_categories$exploitation_pattern, .data[[product_cat_col]], ignore.case = TRUE) &
#           .data[[status_col]] == "Won" ~ 1,
#         TRUE ~ 0
#       )
#     )
#   
#   # Log some stats about flags
#   log_info("Found %d exploration software purchases", 
#            sum(opps_with_flags$exploration_sw_purchase))
#   log_info("Found %d exploitation software purchases", 
#            sum(opps_with_flags$exploitation_sw_purchase))
#   
#   # STEP 1: Basic aggregation in summarise
#   opps_yearly <- opps_with_flags %>%
#     dplyr::group_by(to_customer_id, .data[[close_year_col]]) %>%
#     dplyr::summarise(
#       # Basic opportunity metrics
#       to_number_of_opps = n(),
#       to_sum_opps_closed = sum(
#         .data[[status_col]] %in% c("Won", "Lost", "Lost (autom.)", "Not realized"),
#         na.rm = TRUE
#       ),
#       to_sum_opps_won = sum(.data[[status_col]] == "Won", na.rm = TRUE),
#       to_sum_val_opps_won = sum(
#         ifelse(.data[[status_col]] == "Won", .data[[value_col]], 0),
#         na.rm = TRUE
#       ),
#       
#       # Software purchase indicators
#       to_exploration_sw_purchase = sum(exploration_sw_purchase, na.rm = TRUE),
#       to_exploitation_sw_purchase = sum(exploitation_sw_purchase, na.rm = TRUE),
#       
#       # Product categories - closed opportunities
#       to_closed_accessories = sum(
#         .data[[product_cat_col]] %in% product_categories$t_accessories &
#           .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
#         na.rm = TRUE
#       ),
#       to_closed_hw_mt_standalone = sum(
#         .data[[product_cat_col]] %in% product_categories$t_hw_mt_standalone &
#           .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
#         na.rm = TRUE
#       ),
#       to_closed_hw_lt_standalone = sum(
#         .data[[product_cat_col]] %in% product_categories$t_hw_lt_standalone &
#           .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
#         na.rm = TRUE
#       ),
#       to_closed_software = sum(
#         .data[[product_cat_col]] %in% product_categories$t_software &
#           .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
#         na.rm = TRUE
#       ),
#       to_closed_services = sum(
#         .data[[product_cat_col]] %in% product_categories$t_services &
#           .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
#         na.rm = TRUE
#       ),
#       
#       # Product categories - won value
#       to_value_accessories = sum(
#         ifelse(
#           .data[[product_cat_col]] %in% product_categories$t_accessories & 
#             .data[[status_col]] == "Won",
#           .data[[value_col]], 0
#         ),
#         na.rm = TRUE
#       ),
#       to_value_hw_mt_standalone = sum(
#         ifelse(
#           .data[[product_cat_col]] %in% product_categories$t_hw_mt_standalone & 
#             .data[[status_col]] == "Won",
#           .data[[value_col]], 0
#         ),
#         na.rm = TRUE
#       ),
#       to_value_hw_lt_standalone = sum(
#         ifelse(
#           .data[[product_cat_col]] %in% product_categories$t_hw_lt_standalone & 
#             .data[[status_col]] == "Won",
#           .data[[value_col]], 0
#         ),
#         na.rm = TRUE
#       ),
#       to_value_software = sum(
#         ifelse(
#           .data[[product_cat_col]] %in% product_categories$t_software & 
#             .data[[status_col]] == "Won",
#           .data[[value_col]], 0
#         ),
#         na.rm = TRUE
#       ),
#       to_value_services = sum(
#         ifelse(
#           .data[[product_cat_col]] %in% product_categories$t_services & 
#             .data[[status_col]] == "Won",
#           .data[[value_col]], 0
#         ),
#         na.rm = TRUE
#       ),
#       
#       # Success rate metrics
#       to_opp_winning_probability = ifelse(
#         to_sum_opps_closed == 0,
#         0,
#         to_sum_opps_won / to_sum_opps_closed
#       ),
#       
#       .groups = 'drop'
#     ) %>%
#     # Rename the year column to 'year'
#     dplyr::rename(year = .data[[close_year_col]])
#   
#   # STEP 2: Calculate first purchase years and other derived metrics AFTER renaming
#   opps_yearly <- opps_yearly %>%
#     dplyr::group_by(to_customer_id) %>%
#     dplyr::mutate(
#       # Calculate first purchase year
#       to_first_purchase_year = ifelse(
#         any(to_sum_opps_won > 0),
#         min(year[to_sum_opps_won > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       
#       # Calculate hardware purchase year 
#       to_hw_purchase_year = ifelse(
#         any(to_value_hw_mt_standalone + to_value_hw_lt_standalone > 0),
#         min(year[to_value_hw_mt_standalone + to_value_hw_lt_standalone > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       
#       # Calculate software purchase year
#       to_sw_purchase_year = ifelse(
#         any(to_value_software > 0),
#         min(year[to_value_software > 0], na.rm = TRUE),
#         NA_real_
#       )
#     ) %>%
#     dplyr::ungroup()
#   
#   # Join with panel data and calculate derived metrics
#   panel_data <- panel_data %>%
#     dplyr::left_join(
#       opps_yearly,
#       by = c("customer_id" = "to_customer_id", "year")
#     ) %>%
#     dplyr::group_by(customer_id) %>%
#     dplyr::mutate(
#       # Replace NA with 0 for active relationship periods
#       across(
#         starts_with("to_"),
#         ~case_when(
#           relationship_status == "pre_relationship" ~ NA_real_,
#           relationship_status == "active" ~ coalesce(., 0),
#           TRUE ~ NA_real_
#         )
#       ),
#       
#       # Cumulative metrics
#       to_cum_opps = cumsum(coalesce(to_number_of_opps, 0)),
#       to_cum_opps_won = cumsum(coalesce(to_sum_opps_won, 0)),
#       to_cum_val_opps_won = cumsum(coalesce(to_sum_val_opps_won, 0)),
#       to_cum_exploration_purchase = cumsum(coalesce(to_exploration_sw_purchase, 0)),
#       to_cum_exploitation_purchase = cumsum(coalesce(to_exploitation_sw_purchase, 0)),
#       
#       # Cumulative product category value
#       to_cum_value_accessories = cumsum(coalesce(to_value_accessories, 0)),
#       to_cum_value_hw_mt = cumsum(coalesce(to_value_hw_mt_standalone, 0)),
#       to_cum_value_hw_lt = cumsum(coalesce(to_value_hw_lt_standalone, 0)),
#       to_cum_value_software = cumsum(coalesce(to_value_software, 0)),
#       to_cum_value_services = cumsum(coalesce(to_value_services, 0)),
#       
#       # Calculate first purchase years for different categories
#       first_opp_year = ifelse(
#         sum(to_number_of_opps, na.rm = TRUE) > 0,
#         min(year[to_number_of_opps > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       first_exploration_purchase_year = ifelse(
#         sum(to_exploration_sw_purchase, na.rm = TRUE) > 0,
#         min(year[to_exploration_sw_purchase > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       first_exploitation_purchase_year = ifelse(
#         sum(to_exploitation_sw_purchase, na.rm = TRUE) > 0,
#         min(year[to_exploitation_sw_purchase > 0], na.rm = TRUE),
#         NA_real_
#       )
#     ) %>%
#     # Fill first purchase years down and up
#     tidyr::fill(c(first_opp_year, first_exploration_purchase_year, 
#                   first_exploitation_purchase_year), 
#                 .direction = "downup") %>%
#     dplyr::mutate(
#       # Years since first purchase metrics
#       years_since_first_opp = year - first_opp_year,
#       years_since_first_exploration = ifelse(
#         !is.na(first_exploration_purchase_year),
#         year - first_exploration_purchase_year,
#         NA_real_
#       ),
#       years_since_first_exploitation = ifelse(
#         !is.na(first_exploitation_purchase_year),
#         year - first_exploitation_purchase_year,
#         NA_real_
#       ),
#       
#       # Treatment indicators
#       ever_purchased_exploration = !is.na(first_exploration_purchase_year),
#       ever_purchased_exploitation = !is.na(first_exploitation_purchase_year),
#       
#       post_exploration_period = ifelse(
#         !is.na(first_exploration_purchase_year) & year >= first_exploration_purchase_year,
#         1, 0
#       ),
#       post_exploitation_period = ifelse(
#         !is.na(first_exploitation_purchase_year) & year >= first_exploitation_purchase_year,
#         1, 0
#       ),
#       
#       # Rolling metrics (3-year windows)
#       to_rolling_opps_3yr = zoo::rollmean(
#         coalesce(to_number_of_opps, 0),
#         k = 3, fill = NA, align = "right"
#       ),
#       to_rolling_value_3yr = zoo::rollmean(
#         coalesce(to_sum_val_opps_won, 0),
#         k = 3, fill = NA, align = "right"
#       )
#     ) %>%
#     dplyr::ungroup()
#   
#   log_info("Added opportunity features")
#   
#   if (!"first_exploration_purchase_year" %in% names(panel_data)) {
#     log_warn("first_exploration_purchase_year not created in opportunity features")
#     # Create dummy variable as fallback
#     panel_data$first_exploration_purchase_year <- NA_real_
#     panel_data$post_exploration_period <- 0
#   }
#   
#   return(panel_data)
# }

add_opportunity_features <- function(panel_data, opps_data, product_categories) {
  log_info("Adding opportunity features")
  
  # Check data structure
  check_data_structure(opps_data, "opportunities data")
  check_data_structure(panel_data, "panel data")
  
  # Find or derive required columns for opportunities
  status_col <- find_column_match(
    opps_data, 
    "to_opportunity_status", 
    c("opportunity_status", "status", "state")
  )
  
  product_cat_col <- find_column_match(
    opps_data, 
    "to_product_category", 
    c("product_category", "category", "product_type")
  )
  
  value_col <- find_column_match(
    opps_data, 
    "to_expected_value", 
    c("expected_value", "value", "amount")
  )
  
  close_year_col <- find_column_match(
    opps_data, 
    "to_close_year", 
    c("close_year", "year", "opp_year")
  )
  
  # If close_year is missing but close_date exists, derive it
  if (is.null(close_year_col) && "to_close_date" %in% names(opps_data)) {
    log_info("Deriving to_close_year from to_close_date")
    opps_data <- opps_data %>%
      dplyr::mutate(to_close_year = lubridate::year(to_close_date))
    close_year_col <- "to_close_year"
  }
  
  # Log required columns for reporting
  required_cols <- c(
    "to_customer_id", status_col, product_cat_col, 
    value_col, close_year_col
  )
  
  log_info("Using columns for opportunity features: %s", 
           paste(required_cols, collapse=", "))
  
  # Check if we have the required columns
  if (any(sapply(required_cols, is.null))) {
    missing_cols <- required_cols[sapply(required_cols, is.null)]
    stop(paste("Missing required columns for opportunity features:", 
               paste(missing_cols, collapse=", ")))
  }
  
  # STEP 1: Basic aggregation in summarise - without exploration/exploitation flags
  opps_yearly <- opps_data %>%
    dplyr::mutate(
      to_customer_id = as.character(to_customer_id)
    ) %>%
    dplyr::group_by(to_customer_id, .data[[close_year_col]]) %>%
    dplyr::summarise(
      # Basic opportunity metrics
      to_number_of_opps = n(),
      to_sum_opps_closed = sum(
        .data[[status_col]] %in% c("Won", "Lost", "Lost (autom.)", "Not realized"),
        na.rm = TRUE
      ),
      to_sum_opps_won = sum(.data[[status_col]] == "Won", na.rm = TRUE),
      to_sum_val_opps_won = sum(
        ifelse(.data[[status_col]] == "Won", .data[[value_col]], 0),
        na.rm = TRUE
      ),
      
      # Product categories - closed opportunities
      to_closed_accessories = sum(
        .data[[product_cat_col]] %in% product_categories$t_accessories &
          .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
        na.rm = TRUE
      ),
      to_closed_hw_mt_standalone = sum(
        .data[[product_cat_col]] %in% product_categories$t_hw_mt_standalone &
          .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
        na.rm = TRUE
      ),
      to_closed_hw_lt_standalone = sum(
        .data[[product_cat_col]] %in% product_categories$t_hw_lt_standalone &
          .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
        na.rm = TRUE
      ),
      to_closed_software = sum(
        .data[[product_cat_col]] %in% product_categories$t_software &
          .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
        na.rm = TRUE
      ),
      to_closed_services = sum(
        .data[[product_cat_col]] %in% product_categories$t_services &
          .data[[status_col]] %in% c("Won", "Lost", "Not realized"),
        na.rm = TRUE
      ),
      
      # Product categories - won value
      to_value_accessories = sum(
        ifelse(
          .data[[product_cat_col]] %in% product_categories$t_accessories & 
            .data[[status_col]] == "Won",
          .data[[value_col]], 0
        ),
        na.rm = TRUE
      ),
      to_value_hw_mt_standalone = sum(
        ifelse(
          .data[[product_cat_col]] %in% product_categories$t_hw_mt_standalone & 
            .data[[status_col]] == "Won",
          .data[[value_col]], 0
        ),
        na.rm = TRUE
      ),
      to_value_hw_lt_standalone = sum(
        ifelse(
          .data[[product_cat_col]] %in% product_categories$t_hw_lt_standalone & 
            .data[[status_col]] == "Won",
          .data[[value_col]], 0
        ),
        na.rm = TRUE
      ),
      to_value_software = sum(
        ifelse(
          .data[[product_cat_col]] %in% product_categories$t_software & 
            .data[[status_col]] == "Won",
          .data[[value_col]], 0
        ),
        na.rm = TRUE
      ),
      to_value_services = sum(
        ifelse(
          .data[[product_cat_col]] %in% product_categories$t_services & 
            .data[[status_col]] == "Won",
          .data[[value_col]], 0
        ),
        na.rm = TRUE
      ),
      
      # Success rate metrics
      to_opp_winning_probability = ifelse(
        to_sum_opps_closed == 0,
        0,
        to_sum_opps_won / to_sum_opps_closed
      ),
      
      .groups = 'drop'
    ) %>%
    # Rename the year column to 'year'
    dplyr::rename(year = .data[[close_year_col]])
  
  # STEP 2: Calculate first purchase years and other derived metrics
  opps_yearly <- opps_yearly %>%
    dplyr::group_by(to_customer_id) %>%
    dplyr::mutate(
      # Calculate first purchase year
      to_first_purchase_year = ifelse(
        any(to_sum_opps_won > 0),
        min(year[to_sum_opps_won > 0], na.rm = TRUE),
        NA_real_
      ),
      
      # Calculate hardware purchase year 
      to_hw_purchase_year = ifelse(
        any(to_value_hw_mt_standalone + to_value_hw_lt_standalone > 0),
        min(year[to_value_hw_mt_standalone + to_value_hw_lt_standalone > 0], na.rm = TRUE),
        NA_real_
      ),
      
      # Calculate software purchase year
      to_sw_purchase_year = ifelse(
        any(to_value_software > 0),
        min(year[to_value_software > 0], na.rm = TRUE),
        NA_real_
      )
    ) %>%
    dplyr::ungroup()
  
  # Join with panel data and calculate derived metrics
  panel_data <- panel_data %>%
    dplyr::left_join(
      opps_yearly,
      by = c("customer_id" = "to_customer_id", "year")
    ) %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Replace NA with 0 for active relationship periods
      across(
        starts_with("to_"),
        ~case_when(
          relationship_status == "pre_relationship" ~ NA_real_,
          relationship_status == "active" ~ coalesce(., 0),
          TRUE ~ NA_real_
        )
      ),
      
      # Cumulative metrics
      to_cum_opps = cumsum(coalesce(to_number_of_opps, 0)),
      to_cum_opps_won = cumsum(coalesce(to_sum_opps_won, 0)),
      to_cum_val_opps_won = cumsum(coalesce(to_sum_val_opps_won, 0)),
      
      # Cumulative product category value
      to_cum_value_accessories = cumsum(coalesce(to_value_accessories, 0)),
      to_cum_value_hw_mt = cumsum(coalesce(to_value_hw_mt_standalone, 0)),
      to_cum_value_hw_lt = cumsum(coalesce(to_value_hw_lt_standalone, 0)),
      to_cum_value_software = cumsum(coalesce(to_value_software, 0)),
      to_cum_value_services = cumsum(coalesce(to_value_services, 0)),
      
      # Calculate first purchase years for different categories
      first_opp_year = ifelse(
        sum(to_number_of_opps, na.rm = TRUE) > 0,
        min(year[to_number_of_opps > 0], na.rm = TRUE),
        NA_real_
      )
    ) %>%
    # Fill first purchase years down and up
    tidyr::fill(c(first_opp_year), 
                .direction = "downup") %>%
    dplyr::mutate(
      # Years since first purchase metrics
      years_since_first_opp = year - first_opp_year,
      
      # Rolling metrics (3-year windows)
      to_rolling_opps_3yr = zoo::rollmean(
        coalesce(to_number_of_opps, 0),
        k = 3, fill = NA, align = "right"
      ),
      to_rolling_value_3yr = zoo::rollmean(
        coalesce(to_sum_val_opps_won, 0),
        k = 3, fill = NA, align = "right"
      )
    ) %>%
    dplyr::ungroup()
  
  log_info("Added opportunity features")
  
  return(panel_data)
}

#' Add SAP software purchase features to the panel
#'
#' @param panel_data Panel data
#' @param sap_software_data SAP software data
#' @param customer_data Customer data (for ID mapping)
#' @return Panel with SAP software features added
# add_software_features <- function(panel_data, sap_software_data, customer_data) {
#   log_info("Adding SAP software features")
#   
#   # Check if data is available
#   if (is.null(sap_software_data)) {
#     log_warn("SAP software data is not available, skipping software features")
#     return(panel_data)
#   }
#   
#   # Check data structure
#   check_data_structure(sap_software_data, "SAP software data")
#   
#   # Find required columns
#   customer_id_col <- find_column_match(
#     sap_software_data, 
#     "ts_customer_id", 
#     c("customer_id", "endkunde", "kunde")
#   )
#   
#   year_col <- find_column_match(
#     sap_software_data, 
#     "ts_year", 
#     c("year", "geschaftsjahr", "fin_year")
#   )
#   
#   generation_col <- find_column_match(
#     sap_software_data, 
#     "ts_generation", 
#     c("generation", "x5_generation")
#   )
#   
#   revenue_col <- find_column_match(
#     sap_software_data, 
#     "ts_revenue", 
#     c("revenue", "umsatz")
#   )
#   
#   # Log required columns for reporting
#   required_cols <- c(customer_id_col, year_col, generation_col, revenue_col)
#   log_info("Using columns for software features: %s", paste(required_cols, collapse=", "))
#   
#   # Check if we have the required columns
#   if (any(sapply(required_cols, is.null))) {
#     missing_cols <- required_cols[sapply(required_cols, is.null)]
#     log_warn("Missing required columns for software features: %s", 
#              paste(missing_cols, collapse=", "))
#     return(panel_data)
#   }
#   
#   # Add debug info for exploration pattern detection in SAP data
#   log_info("Checking for exploration patterns in SAP data column: %s", generation_col)
#   log_info("Sample generation values: %s", 
#            paste(head(unique(sap_software_data[[generation_col]]), 10), collapse=", "))
#   
#   # Replicate exactly how exploration software is detected in your engineering script
#   exploration_pattern <- paste(
#     "Quickjob", "Customer", "Monitoring", "Calculate",
#     "Monitor", "Calculation\\(CaaS\\)", "Webcalculation",
#     "Purchasing", "TruTopsMonitor", "Production",
#     "Sonstige Fabrication SW", "Cell", "Equipment Manager",
#     sep = "|"
#   )
#   
#   exploitation_pattern <- paste(
#     "BOOST", "Weld", "Classic", "Tube", "Bend",
#     sep = "|"
#   )
#   
#   # Process SAP software data - match exactly with engineering script
#   sap_sw <- sap_software_data %>%
#     dplyr::mutate(
#       ts_customer_id = as.character(.data[[customer_id_col]]),
#       # From original engineering script for exploration software definition
#       is_exploration_sw = grepl(
#         exploration_pattern,
#         .data[[generation_col]],
#         ignore.case = TRUE
#       ),
#       # From original engineering script for exploitation software definition
#       is_exploitation_sw = grepl(
#         exploitation_pattern,
#         .data[[generation_col]],
#         ignore.case = TRUE
#       )
#     )
#   
#   # Log counts directly from SAP data
#   log_info("Found %d exploration software items in SAP data", 
#            sum(sap_sw$is_exploration_sw, na.rm = TRUE))
#   log_info("Found %d exploitation software items in SAP data", 
#            sum(sap_sw$is_exploitation_sw, na.rm = TRUE))
#   
#   # Aggregate software purchases by customer and year - again matching engineering script
#   sw_yearly <- sap_sw %>%
#     dplyr::group_by(ts_customer_id, .data[[year_col]]) %>%
#     dplyr::summarise(
#       # Total software metrics
#       ts_sales_sw_num_total = n(),
#       ts_sales_total_sw = sum(.data[[revenue_col]], na.rm = TRUE),
#       
#       # Exploration software metrics
#       ts_sales_num_sw_explore = sum(is_exploration_sw, na.rm = TRUE),
#       ts_sales_total_exploration = sum(
#         ifelse(is_exploration_sw, .data[[revenue_col]], 0),
#         na.rm = TRUE
#       ),
#       
#       # Exploitation software metrics
#       ts_sales_num_sw_exploit = sum(is_exploitation_sw, na.rm = TRUE),
#       ts_sales_total_exploitation = sum(
#         ifelse(is_exploitation_sw, .data[[revenue_col]], 0),
#         na.rm = TRUE
#       ),
#       
#       .groups = 'drop'
#     ) %>%
#     # Rename the year column to 'year'
#     dplyr::rename(year = .data[[year_col]])
#   
#   # Check if customer_sap_id exists in panel data for joining
#   join_column <- if ("customer_sap_id" %in% names(panel_data)) {
#     log_info("Joining software data using customer_sap_id")
#     c("customer_sap_id" = "ts_customer_id", "year")
#   } else {
#     log_info("Joining software data using customer_id")
#     c("customer_id" = "ts_customer_id", "year")
#   }
#   
#   # Join with panel data
#   panel_data <- panel_data %>%
#     dplyr::left_join(
#       sw_yearly,
#       by = join_column
#     )
#   
#   # Rest of function remains unchanged...
#   panel_data <- panel_data %>%
#     dplyr::group_by(customer_id) %>%
#     dplyr::mutate(
#       # Handle relationship periods
#       across(
#         starts_with("ts_"),
#         ~case_when(
#           relationship_status == "pre_relationship" ~ NA_real_,
#           relationship_status == "active" ~ coalesce(., 0),
#           TRUE ~ NA_real_
#         )
#       ),
#       
#       # Cumulative software metrics
#       ts_sales_sw_num_total_cum = cumsum(coalesce(ts_sales_sw_num_total, 0)),
#       ts_sales_num_sw_explore_cum = cumsum(coalesce(ts_sales_num_sw_explore, 0)),
#       ts_sales_num_sw_exploit_cum = cumsum(coalesce(ts_sales_num_sw_exploit, 0)),
#       ts_sales_total_sw_cum = cumsum(coalesce(ts_sales_total_sw, 0)),
#       ts_sales_total_exploration_cum = cumsum(coalesce(ts_sales_total_exploration, 0)),
#       ts_sales_total_exploitation_cum = cumsum(coalesce(ts_sales_total_exploitation, 0)),
#       
#       # Software mix metrics
#       ts_exploration_ratio = ifelse(
#         ts_sales_total_sw > 0,
#         ts_sales_total_exploration / ts_sales_total_sw,
#         0
#       ),
#       ts_exploitation_ratio = ifelse(
#         ts_sales_total_sw > 0,
#         ts_sales_total_exploitation / ts_sales_total_sw,
#         0
#       ),
#       
#       # Calculate first purchase years using SAP data
#       ts_first_any_sw_year = ifelse(
#         sum(ts_sales_sw_num_total, na.rm = TRUE) > 0,
#         min(year[ts_sales_sw_num_total > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       ts_first_explore_year = ifelse(
#         sum(ts_sales_num_sw_explore, na.rm = TRUE) > 0,
#         min(year[ts_sales_num_sw_explore > 0], na.rm = TRUE),
#         NA_real_
#       ),
#       ts_first_exploit_year = ifelse(
#         sum(ts_sales_num_sw_exploit, na.rm = TRUE) > 0,
#         min(year[ts_sales_num_sw_exploit > 0], na.rm = TRUE),
#         NA_real_
#       )
#     ) %>%
#     # Fill first purchase years down and up
#     tidyr::fill(c(ts_first_any_sw_year, ts_first_explore_year, ts_first_exploit_year), 
#                 .direction = "downup") %>%
#     dplyr::mutate(
#       # Treatment indicators from SAP data
#       ts_ever_purchased_sw = !is.na(ts_first_any_sw_year),
#       ts_ever_purchased_explore = !is.na(ts_first_explore_year),
#       ts_ever_purchased_exploit = !is.na(ts_first_exploit_year),
#       
#       # Post-treatment period indicators
#       ts_post_any_sw_period = ifelse(
#         !is.na(ts_first_any_sw_year) & year >= ts_first_any_sw_year,
#         1, 0
#       ),
#       ts_post_explore_period = ifelse(
#         !is.na(ts_first_explore_year) & year >= ts_first_explore_year,
#         1, 0
#       ),
#       ts_post_exploit_period = ifelse(
#         !is.na(ts_first_exploit_year) & year >= ts_first_exploit_year,
#         1, 0
#       ),
#       
#       # Years since first purchase metrics from SAP data
#       ts_years_since_any_sw = ifelse(
#         !is.na(ts_first_any_sw_year),
#         year - ts_first_any_sw_year,
#         NA_real_
#       ),
#       ts_years_since_explore = ifelse(
#         !is.na(ts_first_explore_year),
#         year - ts_first_explore_year,
#         NA_real_
#       ),
#       ts_years_since_exploit = ifelse(
#         !is.na(ts_first_exploit_year),
#         year - ts_first_exploit_year,
#         NA_real_
#       )
#     ) %>%
#     dplyr::ungroup()
#   
#   log_info("Added SAP software features")
#   
#   if (!"ts_first_explore_year" %in% names(panel_data)) {
#     log_warn("ts_first_explore_year not created in software features")
#     # Create dummy variable as fallback
#     panel_data$ts_first_explore_year <- NA_real_
#     panel_data$ts_post_explore_period <- 0
#   }
#   
#   return(panel_data)
# }

add_software_features <- function(panel_data, sap_software_data, customer_data) {
  log_info("Adding SAP software features")
  
  if (is.null(sap_software_data)) {
    log_warn("SAP software data is not available, skipping software features")
    return(panel_data)
  }
  
  # Define exploration pattern exactly as in the engineering script
  exploration_pattern <- paste(
    "Quickjob", "Customer", "Monitoring", "Calculate",
    "Monitor", "Calculation\\(CaaS\\)", "Webcalculation",
    "Purchasing", "TruTopsMonitor", "Production",
    "Sonstige Fabrication SW", "Cell", "Equipment Manager",
    sep = "|"
  )
  
  # Find required columns
  customer_id_col <- find_column_match(
    sap_software_data, 
    "ts_customer_id", 
    c("customer_id", "endkunde", "kunde")
  )
  
  year_col <- find_column_match(
    sap_software_data, 
    "ts_year", 
    c("year", "geschaftsjahr", "fin_year")
  )
  
  generation_col <- find_column_match(
    sap_software_data, 
    "ts_generation", 
    c("generation", "x5_generation")
  )
  
  revenue_col <- find_column_match(
    sap_software_data, 
    "ts_revenue", 
    c("revenue", "umsatz")
  )
  
  # Process SAP software data - replicating the engineering script approach
  sap_sw <- sap_software_data %>%
    dplyr::mutate(
      ts_customer_id = as.character(.data[[customer_id_col]]),
      # Identify exploration software exactly as in engineering script
      is_exploration_sw = grepl(
        exploration_pattern,
        .data[[generation_col]],
        ignore.case = TRUE
      ),
      # Identify exploitation software
      is_exploitation_sw = grepl(
        "BOOST|Weld|Classic|Tube|Bend",
        .data[[generation_col]],
        ignore.case = TRUE
      )
    )
  
  # Create yearly aggregates for software purchases
  sw_yearly <- sap_sw %>%
    dplyr::group_by(ts_customer_id, .data[[year_col]]) %>%
    dplyr::summarise(
      # Total software metrics
      ts_sales_sw_num_total = n(),
      ts_sales_total_sw = sum(.data[[revenue_col]], na.rm = TRUE),
      
      # Exploration software metrics
      ts_sales_num_sw_explore = sum(is_exploration_sw, na.rm = TRUE),
      ts_sales_total_exploration = sum(
        ifelse(is_exploration_sw, .data[[revenue_col]], 0),
        na.rm = TRUE
      ),
      
      # Exploitation software metrics
      ts_sales_num_sw_exploit = sum(is_exploitation_sw, na.rm = TRUE),
      ts_sales_total_exploitation = sum(
        ifelse(is_exploitation_sw, .data[[revenue_col]], 0),
        na.rm = TRUE
      ),
      
      .groups = 'drop'
    ) %>%
    dplyr::rename(year = .data[[year_col]])
  
  # Log some stats about exploration purchases
  log_info("Found %d exploration software purchases", 
           sum(sw_yearly$ts_sales_num_sw_explore, na.rm = TRUE))
  
  # Join with panel data
  join_column <- if ("customer_sap_id" %in% names(panel_data)) {
    log_info("Joining software data using customer_sap_id")
    c("customer_sap_id" = "ts_customer_id", "year")
  } else {
    log_info("Joining software data using customer_id")
    c("customer_id" = "ts_customer_id", "year")
  }
  
  panel_data <- panel_data %>%
    dplyr::left_join(
      sw_yearly,
      by = join_column
    )
  
  # Calculate cumulative metrics and first purchase years
  panel_data <- panel_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Replace NAs with 0s for active relationship periods
      across(
        starts_with("ts_"),
        ~case_when(
          relationship_status == "pre_relationship" ~ NA_real_,
          relationship_status == "active" ~ coalesce(., 0),
          TRUE ~ NA_real_
        )
      ),
      
      # Cumulative metrics
      ts_sales_sw_num_total_cum = cumsum(coalesce(ts_sales_sw_num_total, 0)),
      ts_sales_num_sw_explore_cum = cumsum(coalesce(ts_sales_num_sw_explore, 0)),
      ts_sales_num_sw_exploit_cum = cumsum(coalesce(ts_sales_num_sw_exploit, 0)),
      ts_sales_total_sw_cum = cumsum(coalesce(ts_sales_total_sw, 0)),
      ts_sales_total_exploration_cum = cumsum(coalesce(ts_sales_total_exploration, 0)),
      ts_sales_total_exploitation_cum = cumsum(coalesce(ts_sales_total_exploitation, 0)),
      
      # First purchase years - critical for treatment identification
      ts_first_any_sw_year = ifelse(
        sum(ts_sales_sw_num_total, na.rm = TRUE) > 0,
        min(year[ts_sales_sw_num_total > 0], na.rm = TRUE),
        NA_real_
      ),
      ts_first_explore_year = ifelse(
        sum(ts_sales_num_sw_explore, na.rm = TRUE) > 0,
        min(year[ts_sales_num_sw_explore > 0], na.rm = TRUE),
        NA_real_
      ),
      ts_first_exploit_year = ifelse(
        sum(ts_sales_num_sw_exploit, na.rm = TRUE) > 0,
        min(year[ts_sales_num_sw_exploit > 0], na.rm = TRUE),
        NA_real_
      )
    )
  
  # Fill first purchase years down and up within each customer
  panel_data <- panel_data %>%
    tidyr::fill(c(ts_first_any_sw_year, ts_first_explore_year, ts_first_exploit_year), 
                .direction = "downup") %>%
    dplyr::mutate(
      # Treatment indicators
      ts_ever_purchased_sw = !is.na(ts_first_any_sw_year),
      ts_ever_purchased_explore = !is.na(ts_first_explore_year),
      ts_ever_purchased_exploit = !is.na(ts_first_exploit_year),
      
      # Post-treatment period indicators - these are crucial
      ts_post_any_sw_period = ifelse(
        !is.na(ts_first_any_sw_year) & year >= ts_first_any_sw_year,
        1, 0
      ),
      ts_post_explore_period = ifelse(
        !is.na(ts_first_explore_year) & year >= ts_first_explore_year,
        1, 0
      ),
      ts_post_exploit_period = ifelse(
        !is.na(ts_first_exploit_year) & year >= ts_first_exploit_year,
        1, 0
      ),
      
      # Years since first purchase metrics - these will be used in time_to_treat
      ts_years_since_any_sw = ifelse(
        !is.na(ts_first_any_sw_year),
        year - ts_first_any_sw_year,
        NA_real_
      ),
      ts_years_since_explore = ifelse(
        !is.na(ts_first_explore_year),
        year - ts_first_explore_year,
        NA_real_
      ),
      ts_years_since_exploit = ifelse(
        !is.na(ts_first_exploit_year),
        year - ts_first_exploit_year,
        NA_real_
      )
    ) %>%
    dplyr::ungroup()
  
  log_info("Added SAP software features")
  
  # Debug
  if("ts_first_explore_year" %in% names(panel_data)) {
    num_explore_purchasers <- sum(!is.na(panel_data$ts_first_explore_year))
    log_info("Found %d customers with exploration software purchases", 
             length(unique(panel_data$customer_id[!is.na(panel_data$ts_first_explore_year)])))
  } else {
    log_warn("ts_first_explore_year not created in software features")
  }
  
  return(panel_data)
}

#' Add interaction features to the panel
#'
#' @param panel_data Panel data
#' @param meetings_data Meetings data
#' @param mails_data Mails data
#' @param other_act_data Other activities data
#' @return Panel with interaction features added
add_interaction_features <- function(panel_data, meetings_data, mails_data, other_act_data) {
  log_info("Adding interaction features")
  
  # Create a helper function to ensure columns exist
  ensure_columns <- function(df, columns, default_value = 0) {
    for (col in columns) {
      if (!col %in% names(df)) {
        df[[col]] <- default_value
        log_info("Created missing column %s with default value %s", col, default_value)
      }
    }
    return(df)
  }
  
  # Process meetings data
  if (!is.null(meetings_data)) {
    # First check what columns are actually available
    log_info("Columns in meetings_data: %s", paste(head(names(meetings_data), 10), collapse=", "))
    
    account_id_col <- find_column_match(
      meetings_data, 
      "account_id", 
      c("account", "customer_id", "id")
    )
    
    year_col <- find_column_match(
      meetings_data, 
      "meeting_starting_year", 
      c("year", "start_year", "meeting_year")
    )
    
    status_col <- find_column_match(
      meetings_data, 
      "status", 
      c("meeting_status", "state")
    )
    
    duration_col <- find_column_match(
      meetings_data, 
      "meeting_duration_minutes", 
      c("duration", "duration_minutes", "length")
    )
    
    # If we have at least account and year, proceed with aggregation
    if (!is.null(account_id_col) && !is.null(year_col)) {
      meetings_agg <- meetings_data %>%
        dplyr::mutate(
          account_id = as.character(.data[[account_id_col]]),
          year = as.numeric(.data[[year_col]])
        ) %>%
        dplyr::filter(!is.na(year) & !is.na(account_id)) %>%
        dplyr::group_by(account_id, year) %>%
        dplyr::summarise(
          # Total meetings count
          completed_meetings_num_total = if (!is.null(status_col)) {
            sum(.data[[status_col]] == "Completed", na.rm = TRUE)
          } else {
            n()  # If no status column, count all meetings
          },
          # Meeting duration if available
          total_meeting_duration = if (!is.null(duration_col)) {
            sum(.data[[duration_col]], na.rm = TRUE)
          } else {
            0  # Default if no duration info
          },
          .groups = 'drop'
        )
      
      # Only join if we have data
      if (nrow(meetings_agg) > 0) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            meetings_agg,
            by = c("customer_id" = "account_id", "year")
          )
        log_info("Added meetings data: %d rows", nrow(meetings_agg))
      } else {
        log_warn("No valid meetings data to join")
      }
    } else {
      log_warn("Missing required columns (account_id or year) for meetings data")
    }
  }
  
  # Process emails data
  if (!is.null(mails_data)) {
    # First check what columns are actually available
    log_info("Columns in mails_data: %s", paste(head(names(mails_data), 10), collapse=", "))
    
    account_id_col <- find_column_match(
      mails_data, 
      "account_id", 
      c("account", "customer_id", "id")
    )
    
    year_col <- find_column_match(
      mails_data, 
      "sent_year", 
      c("year", "mail_year")
    )
    
    # Try deriving year from sent_on if year column doesn't exist
    if (is.null(year_col) && "sent_on" %in% names(mails_data) && inherits(mails_data$sent_on, "Date")) {
      log_info("Deriving year from sent_on date column")
      mails_data <- mails_data %>%
        dplyr::mutate(sent_year = lubridate::year(sent_on))
      year_col <- "sent_year"
    }
    
    priority_col <- find_column_match(
      mails_data, 
      "priority", 
      c("mail_priority", "importance")
    )
    
    # If we have at least account and year, proceed with aggregation
    if (!is.null(account_id_col) && !is.null(year_col)) {
      mails_agg <- mails_data %>%
        dplyr::mutate(
          account_id = as.character(.data[[account_id_col]]),
          year = as.numeric(.data[[year_col]])
        ) %>%
        dplyr::filter(!is.na(year) & !is.na(account_id)) %>%
        dplyr::group_by(account_id, year) %>%
        dplyr::summarise(
          mails_total = n(),
          # Priority counts if available
          mails_high_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] %in% c("Urgent", "High"), na.rm = TRUE)
          } else {
            0
          },
          mails_normal_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] %in% c("Normal", "Medium"), na.rm = TRUE)
          } else {
            0
          },
          mails_low_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] %in% c("Low"), na.rm = TRUE)
          } else {
            0
          },
          .groups = 'drop'
        )
      
      # Only join if we have data
      if (nrow(mails_agg) > 0) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            mails_agg,
            by = c("customer_id" = "account_id", "year")
          )
        log_info("Added mails data: %d rows", nrow(mails_agg))
      } else {
        log_warn("No valid mails data to join")
      }
    } else {
      log_warn("Missing required columns (account_id or year) for mails data")
    }
  }
  
  # Process other activities data
  if (!is.null(other_act_data)) {
    # First log what columns are actually available
    log_info("Columns in other_act_data: %s", paste(names(other_act_data), collapse=", "))
    
    # Find the appropriate account column - try several possibilities
    account_col_candidates <- c("id", "account", "account_id", "account_name", "customer", "customer_name")
    account_col <- find_column_match(
      other_act_data, 
      "account", 
      account_col_candidates
    )
    
    # Find the activity type column
    activity_type_candidates <- c("acitivy_type", "activity_type", "type")
    activity_type_col <- find_column_match(
      other_act_data, 
      "acitivy_type", 
      activity_type_candidates
    )
    
    # Find the year column
    year_col_candidates <- c("activity_year", "year", "act_year")
    year_col <- find_column_match(
      other_act_data, 
      "activity_year", 
      year_col_candidates
    )
    
    # Create the aggregation if we have the necessary columns
    if (!is.null(account_col) && !is.null(year_col)) {
      other_act_agg <- other_act_data %>%
        dplyr::mutate(
          account = as.character(.data[[account_col]]),
          year = as.numeric(.data[[year_col]])
        ) %>%
        dplyr::filter(!is.na(year)) %>%
        dplyr::group_by(account, year) %>%
        dplyr::summarise(
          other_act_total = n(),
          # Activity type counts if available
          other_act_phone_calls = if (!is.null(activity_type_col)) {
            sum(grepl("Phone Call", .data[[activity_type_col]]), na.rm = TRUE)
          } else {
            0
          },
          other_act_letters = if (!is.null(activity_type_col)) {
            sum(grepl("Letter", .data[[activity_type_col]]), na.rm = TRUE)
          } else {
            0
          },
          other_act_faxes = if (!is.null(activity_type_col)) {
            sum(grepl("Fax", .data[[activity_type_col]]), na.rm = TRUE)
          } else {
            0
          },
          .groups = 'drop'
        )
      
      # Determine which join column to use - customer_name or customer_id
      if ("customer_name" %in% names(panel_data)) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            other_act_agg,
            by = c("customer_name" = "account", "year")
          )
        log_info("Joined other activities using customer_name")
      } else if ("customer_id" %in% names(panel_data)) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            other_act_agg,
            by = c("customer_id" = "account", "year")
          )
        log_info("Joined other activities using customer_id")
      } else {
        log_warn("Could not join other activities - no suitable join column in panel data")
      }
    } else {
      if (is.null(account_col)) {
        log_warn("No suitable account column found in other_act_data. Tried: %s", 
                 paste(account_col_candidates, collapse=", "))
      }
      if (is.null(year_col)) {
        log_warn("No suitable year column found in other_act_data. Tried: %s", 
                 paste(year_col_candidates, collapse=", "))
      }
    }
  }
  
  # Ensure required columns exist before calculations
  required_columns <- c(
    "completed_meetings_num_total", "total_meeting_duration", 
    "mails_total", "mails_high_priority", "mails_normal_priority", "mails_low_priority",
    "other_act_total", "other_act_phone_calls", "other_act_letters", "other_act_faxes"
  )
  
  panel_data <- ensure_columns(panel_data, required_columns)
  
  # Calculate aggregate interaction metrics
  panel_data <- panel_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Handle relationship periods
      across(
        all_of(required_columns),
        ~case_when(
          relationship_status == "pre_relationship" ~ NA_real_,
          relationship_status == "active" ~ coalesce(., 0),
          TRUE ~ NA_real_
        )
      ),
      
      # Calculate synchronous communications (meetings, phone calls)
      sales_act_synchronous = as.numeric(coalesce(completed_meetings_num_total, 0)) + 
        as.numeric(coalesce(other_act_phone_calls, 0)),
      
      # Calculate asynchronous communications (emails, letters, faxes)
      sales_act_asynchronous = as.numeric(coalesce(mails_total, 0)) + 
        as.numeric(coalesce(other_act_letters, 0)) + 
        as.numeric(coalesce(other_act_faxes, 0)),
      
      # Total activities
      aggregated_sales_act = sales_act_synchronous + sales_act_asynchronous
    )
  
  # Add effort to revenue ratio calculations if to_sum_val_opps_won exists
  if ("to_sum_val_opps_won" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        # Calculate effort to revenue ratio
        effort_to_revenue_ratio = 1000 * aggregated_sales_act / 
          ifelse(to_sum_val_opps_won > 0, to_sum_val_opps_won, 1),
        
        # Handle Inf and NaN values
        effort_to_revenue_ratio = ifelse(
          is.infinite(effort_to_revenue_ratio) | is.nan(effort_to_revenue_ratio),
          0,
          effort_to_revenue_ratio
        ),
        
        # Calculate lagged effort to revenue ratio
        lagged_effort_to_revenue_ratio = 1000 * (lag(sales_act_synchronous, 1) + 
                                                   lag(sales_act_asynchronous, 1)) / 
          ifelse(to_sum_val_opps_won > 0, to_sum_val_opps_won, 1),
        
        # Handle Inf and NaN values
        lagged_effort_to_revenue_ratio = ifelse(
          is.infinite(lagged_effort_to_revenue_ratio) | 
            is.nan(lagged_effort_to_revenue_ratio),
          0,
          lagged_effort_to_revenue_ratio
        )
      )
  } else {
    # Create placeholder columns
    panel_data <- panel_data %>%
      dplyr::mutate(
        effort_to_revenue_ratio = 0,
        lagged_effort_to_revenue_ratio = 0
      )
    
    log_warn("to_sum_val_opps_won column not found, created placeholder revenue ratio columns")
  }
  
  # Add rolling metrics if we have enough data
  if (nrow(panel_data) > 3) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        # Calculate safely using tryCatch
        rolling_interactions_3yr = tryCatch(
          zoo::rollmean(aggregated_sales_act, k = 3, fill = NA, align = "right"),
          error = function(e) {
            log_warn("Error calculating rolling interactions: %s", e$message)
            rep(NA_real_, length(aggregated_sales_act))
          }
        ),
        
        rolling_efforts_3yr = tryCatch(
          zoo::rollmean(effort_to_revenue_ratio, k = 3, fill = NA, align = "right"),
          error = function(e) {
            log_warn("Error calculating rolling efforts: %s", e$message)
            rep(NA_real_, length(effort_to_revenue_ratio))
          }
        )
      )
  } else {
    panel_data$rolling_interactions_3yr <- NA_real_
    panel_data$rolling_efforts_3yr <- NA_real_
    log_warn("Too few rows to calculate rolling metrics")
  }
  
  panel_data <- dplyr::ungroup(panel_data)
  
  log_info("Added interaction features")
  return(panel_data)
}

#' Add service-related features to the panel
#'
#' @param panel_data Panel data
#' @param sis_cases_data SiS cases data
#' @param sis_missions_data SiS missions data
#' @param customer_data Customer data (for ID mapping)
#' @return Panel with service features added
add_service_features <- function(panel_data, sis_cases_data, sis_missions_data, customer_data) {
  log_info("Adding service features")
  
  # Process cases data
  if (!is.null(sis_cases_data)) {
    # Log available columns
    log_info("Columns in sis_cases_data: %s", paste(head(names(sis_cases_data), 10), collapse=", "))
    
    # Find required columns
    customer_col <- find_column_match(
      sis_cases_data, 
      "sc_customer_number", 
      c("customer_number", "customer_id", "account_id")
    )
    
    year_col <- find_column_match(
      sis_cases_data, 
      "case_year", 
      c("year", "service_year")
    )
    
    case_type_col <- find_column_match(
      sis_cases_data, 
      "sc_case_type", 
      c("case_type", "type")
    )
    
    priority_col <- find_column_match(
      sis_cases_data, 
      "sc_priority", 
      c("priority", "case_priority")
    )
    
    reaction_time_col <- find_column_match(
      sis_cases_data, 
      "sc_reaction_time", 
      c("reaction_time", "response_time")
    )
    
    delayed_callbacks_col <- find_column_match(
      sis_cases_data, 
      "sc_delayed_callbacks", 
      c("delayed_callbacks", "late_callbacks")
    )
    
    # If we have at least customer and year columns, proceed
    if (!is.null(customer_col) && !is.null(year_col)) {
      cases_agg <- sis_cases_data %>%
        dplyr::mutate(
          sc_customer_number = as.character(.data[[customer_col]]),
          year = .data[[year_col]]
        ) %>%
        dplyr::filter(!is.na(year)) %>%
        dplyr::group_by(sc_customer_number, year) %>%
        dplyr::summarise(
          # Case counts
          n_cases = n(),
          
          # Case types if available
          n_breakdowns = if (!is.null(case_type_col)) {
            sum(.data[[case_type_col]] == "Breakdown", na.rm = TRUE)
          } else {
            0
          },
          n_complaints = if (!is.null(case_type_col)) {
            sum(.data[[case_type_col]] == "Complaint", na.rm = TRUE)
          } else {
            0
          },
          n_failures = if (!is.null(case_type_col)) {
            sum(.data[[case_type_col]] == "Failure", na.rm = TRUE)
          } else {
            0
          },
          
          # Priority metrics if available
          n_high_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] == "High", na.rm = TRUE)
          } else {
            0
          },
          n_default_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] == "Default", na.rm = TRUE)
          } else {
            0
          },
          n_low_priority = if (!is.null(priority_col)) {
            sum(.data[[priority_col]] == "Low", na.rm = TRUE)
          } else {
            0
          },
          
          # Service quality indicators if available
          avg_reaction_time = if (!is.null(reaction_time_col)) {
            mean(.data[[reaction_time_col]], na.rm = TRUE)
          } else {
            NA_real_
          },
          n_delayed_callbacks = if (!is.null(delayed_callbacks_col)) {
            sum(.data[[delayed_callbacks_col]] > 0, na.rm = TRUE)
          } else {
            0
          },
          
          # Dissatisfier metrics if both case type and priority are available
          Low_service_dissatisfiers_sum = if (!is.null(case_type_col) && !is.null(priority_col)) {
            sum(
              (.data[[case_type_col]] %in% c("Breakdown", "Complaint", "Failure", "Spare part return")) & 
                .data[[priority_col]] == "Low", 
              na.rm = TRUE
            )
          } else {
            0
          },
          
          Default_service_dissatisfiers_sum = if (!is.null(case_type_col) && !is.null(priority_col)) {
            sum(
              (.data[[case_type_col]] %in% c("Breakdown", "Complaint", "Failure", "Spare part return")) & 
                .data[[priority_col]] == "Default", 
              na.rm = TRUE
            )
          } else {
            0
          },
          
          High_service_dissatisfiers_sum = if (!is.null(case_type_col) && !is.null(priority_col)) {
            sum(
              (.data[[case_type_col]] %in% c("Breakdown", "Complaint", "Failure", "Spare part return")) & 
                .data[[priority_col]] == "High", 
              na.rm = TRUE
            )
          } else {
            0
          },
          
          .groups = 'drop'
        ) %>%
        dplyr::mutate(
          all_dissatisfaction = Low_service_dissatisfiers_sum + 
            Default_service_dissatisfiers_sum + 
            High_service_dissatisfiers_sum
        )
      
      # Determine join column
      if ("customer_sap_id" %in% names(panel_data)) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            cases_agg,
            by = c("customer_sap_id" = "sc_customer_number", "year")
          )
        log_info("Joined cases data using customer_sap_id")
      } else {
        panel_data <- panel_data %>%
          dplyr::left_join(
            cases_agg,
            by = c("customer_id" = "sc_customer_number", "year")
          )
        log_info("Joined cases data using customer_id")
      }
    } else {
      log_warn("Missing required columns (customer or year) for cases data")
    }
  }
  
  # Process missions data if available
  if (!is.null(sis_missions_data)) {
    # Find relevant columns
    mission_id_col <- find_column_match(
      sis_missions_data, 
      "mission_id", 
      c("id", "mission", "case_id")
    )
    
    year_col <- find_column_match(
      sis_missions_data, 
      "mission_year", 
      c("year", "service_year")
    )
    
    waiting_time_col <- find_column_match(
      sis_missions_data, 
      "waiting_time_days", 
      c("waiting_time", "wait_days")
    )
    
    working_time_col <- find_column_match(
      sis_missions_data, 
      "working_time_h", 
      c("working_time", "work_hours")
    )
    
    delayed_col <- find_column_match(
      sis_missions_data, 
      "missions_delayed", 
      c("delayed", "is_delayed")
    )
    
    # If we have id and year, proceed
    if (!is.null(mission_id_col) && !is.null(year_col)) {
      missions_agg <- sis_missions_data %>%
        dplyr::mutate(
          mission_id = as.character(.data[[mission_id_col]]),
          year = .data[[year_col]]
        ) %>%
        dplyr::filter(!is.na(year)) %>%
        dplyr::group_by(mission_id, year) %>%
        dplyr::summarise(
          n_missions = n(),
          
          avg_waiting_time = if (!is.null(waiting_time_col)) {
            mean(.data[[waiting_time_col]], na.rm = TRUE)
          } else {
            NA_real_
          },
          
          avg_working_time = if (!is.null(working_time_col)) {
            mean(.data[[working_time_col]], na.rm = TRUE)
          } else {
            NA_real_
          },
          
          n_delayed_missions = if (!is.null(delayed_col)) {
            sum(.data[[delayed_col]] == "Mission delayed", na.rm = TRUE)
          } else {
            0
          },
          
          .groups = 'drop'
        )
      
      # Join with panel data - using mission_id as customer_id here
      # This might need adjustment based on your data structure
      panel_data <- panel_data %>%
        dplyr::left_join(
          missions_agg,
          by = c("customer_id" = "mission_id", "year")
        )
      log_info("Joined missions data using customer_id")
    } else {
      log_warn("Missing required columns (mission_id or year) for missions data")
    }
  }
  
  # Process the joined data
  panel_data <- panel_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Handle relationship periods
      across(
        c(starts_with("n_"), starts_with("avg_"), starts_with("Low_"),
          starts_with("Default_"), starts_with("High_"), "all_dissatisfaction"),
        ~case_when(
          relationship_status == "pre_relationship" ~ NA_real_,
          relationship_status == "active" ~ coalesce(., 0),
          TRUE ~ NA_real_
        )
      ),
      
      # Calculate service intensity metrics if customer_tenure exists
      service_case_intensity = if ("customer_tenure" %in% names(panel_data)) {
        n_cases / coalesce(customer_tenure, 1)
      } else {
        n_cases  # Fallback if tenure not available
      },
      
      service_problem_intensity = (n_breakdowns + n_complaints + n_failures) / 
        ifelse(n_cases > 0, n_cases, 1),
      
      # Calculate 5-year rolling average metrics (from original script)
      High_service_dissatisfiers_sum_ra_5 = zoo::rollmean(
        High_service_dissatisfiers_sum, 
        k = 5, fill = NA, align = "right"
      ),
      
      all_dissatisfaction_ra_5 = zoo::rollmean(
        all_dissatisfaction,
        k = 5, fill = NA, align = "right"
      )
    ) %>%
    dplyr::ungroup()
  
  log_info("Added service features")
  return(panel_data)
}

#' Add installed base features to the panel
#'
#' @param panel_data Panel data
#' @param sis_installed_base_data SiS installed base data
#' @param reg_products_data Registered products data
#' @param customer_data Customer data (for ID mapping)
#' @return Panel with installed base features added
add_installed_base_features <- function(panel_data, sis_installed_base_data, 
                                        reg_products_data, customer_data) {
  log_info("Adding installed base features")
  
  # Process installed base data
  if (!is.null(sis_installed_base_data)) {
    # Find required columns
    customer_col <- find_column_match(
      sis_installed_base_data, 
      "sis_customer_number", 
      c("customer_number", "customer_id", "account_id")
    )
    
    year_col <- find_column_match(
      sis_installed_base_data, 
      "construction_year", 
      c("year", "install_year", "start_year")
    )
    
    equipment_type_col <- find_column_match(
      sis_installed_base_data, 
      "sis_equipment_type", 
      c("equipment_type", "type")
    )
    
    equipment_group_col <- find_column_match(
      sis_installed_base_data, 
      "sis_equipment_group", 
      c("equipment_group", "group")
    )
    
    # If we have customer and year, proceed
    if (!is.null(customer_col) && !is.null(year_col)) {
      installed_hw <- sis_installed_base_data %>%
        dplyr::mutate(
          sis_customer_number = as.character(.data[[customer_col]]),
          year = .data[[year_col]]
        ) %>%
        dplyr::filter(!is.na(year)) %>%
        dplyr::group_by(sis_customer_number, year) %>%
        dplyr::summarise(
          installed_hw_power_bi = n(),
          
          n_equipment_types = if (!is.null(equipment_type_col)) {
            n_distinct(.data[[equipment_type_col]], na.rm = TRUE)
          } else {
            1  # Default to 1 if no type info
          },
          
          n_equipment_groups = if (!is.null(equipment_group_col)) {
            n_distinct(.data[[equipment_group_col]], na.rm = TRUE)
          } else {
            1  # Default to 1 if no group info
          },
          
          .groups = 'drop'
        ) %>%
        dplyr::group_by(sis_customer_number) %>%
        dplyr::arrange(sis_customer_number, year) %>%
        dplyr::mutate(
          installed_hw_cum = cumsum(installed_hw_power_bi)
        ) %>%
        dplyr::ungroup()
      
      # Join with panel data
      if ("customer_sap_id" %in% names(panel_data)) {
        panel_data <- panel_data %>%
          dplyr::left_join(
            installed_hw,
            by = c("customer_sap_id" = "sis_customer_number", "year")
          )
        log_info("Joined installed base data using customer_sap_id")
      } else {
        panel_data <- panel_data %>%
          dplyr::left_join(
            installed_hw,
            by = c("customer_id" = "sis_customer_number", "year")
          )
        log_info("Joined installed base data using customer_id")
      }
    } else {
      log_warn("Missing required columns (customer or year) for installed base data")
    }
  }
  
  # Process registered products data
  if (!is.null(reg_products_data)) {
    # Find required columns
    customer_col <- find_column_match(
      reg_products_data, 
      "customer_id_reg_prod", 
      c("customer_id", "account_id", "id")
    )
    
    year_col <- find_column_match(
      reg_products_data, 
      "completion_year_reg_prod", 
      c("year", "completion_year", "end_year")
    )
    
    category_col <- find_column_match(
      reg_products_data, 
      "registered_product_category_reg_prod", 
      c("product_category", "category")
    )
    
    # If we have customer and year, proceed
    if (!is.null(customer_col) && !is.null(year_col)) {
      reg_prods <- reg_products_data %>%
        dplyr::mutate(
          customer_id_reg_prod = as.character(.data[[customer_col]]),
          year = .data[[year_col]]
        ) %>%
        dplyr::filter(!is.na(year) & year <= 2020) %>%  # Filter out future years
        dplyr::group_by(customer_id_reg_prod, year) %>%
        dplyr::summarise(
          reg_prods_total = n(),
          
          # Count by product registration type
          ERP_Registered_Product = if (!is.null(category_col)) {
            sum(grepl("ERP Registered Product", .data[[category_col]]), na.rm = TRUE)
          } else {
            0
          },
          
          Manual_Registered_Product = if (!is.null(category_col)) {
            sum(grepl("Manual Registered Product", .data[[category_col]]), na.rm = TRUE)
          } else {
            0
          },
          
          Registered_Competitor_Product = if (!is.null(category_col)) {
            sum(grepl("Registered Competitor Product", .data[[category_col]]), na.rm = TRUE)
          } else {
            0
          },
          
          .groups = 'drop'
        )
      
      # Join with panel data
      panel_data <- panel_data %>%
        dplyr::left_join(
          reg_prods,
          by = c("customer_id" = "customer_id_reg_prod", "year")
        )
      log_info("Joined registered products data using customer_id")
      
      # Calculate additional metrics
      panel_data <- panel_data %>%
        dplyr::group_by(customer_id) %>%
        dplyr::mutate(
          # Handle relationship periods
          across(
            c(starts_with("reg_prods_"), starts_with("ERP_"), starts_with("Manual_"),
              starts_with("Registered_"), "installed_hw_power_bi", "installed_hw_cum"),
            ~case_when(
              relationship_status == "pre_relationship" ~ NA_real_,
              relationship_status == "active" ~ coalesce(., 0),
              TRUE ~ NA_real_
            )
          ),
          
          # Calculate cumulative registered products
          reg_prods_total_cum = cumsum(coalesce(reg_prods_total, 0)),
          erp_registered_product_cum = cumsum(coalesce(ERP_Registered_Product, 0)),
          Manual_Registered_Product_cum = cumsum(coalesce(Manual_Registered_Product, 0)),
          Registered_Competitor_Product_cum = cumsum(coalesce(Registered_Competitor_Product, 0)),
          
          # Calculate share metrics
          all_T_reg_products_in_year = coalesce(ERP_Registered_Product, 0) + 
            coalesce(Manual_Registered_Product, 0),
          
          share_of_purchase_T_in_year = ifelse(
            reg_prods_total > 0,
            all_T_reg_products_in_year / reg_prods_total,
            0
          ),
          
          share_of_purchase_competitor_in_year = ifelse(
            reg_prods_total > 0,
            coalesce(Registered_Competitor_Product, 0) / reg_prods_total,
            0
          ),
          
          # Calculate cumulative share metrics
          all_T_reg_products_cum = erp_registered_product_cum + Manual_Registered_Product_cum,
          
          share_of_purchase_T_cum = ifelse(
            reg_prods_total_cum > 0,
            all_T_reg_products_cum / reg_prods_total_cum,
            0
          ),
          
          share_of_purchase_competitor_cum = ifelse(
            reg_prods_total_cum > 0,
            Registered_Competitor_Product_cum / reg_prods_total_cum,
            0
          ),
          
          # Calculate 5-year rolling average metrics
          reg_prods_total_ra_5 = zoo::rollmean(
            reg_prods_total,
            k = 5, fill = NA, align = "right"
          ),
          
          share_of_purchase_T_cum_ra_5 = zoo::rollmean(
            share_of_purchase_T_cum,
            k = 5, fill = NA, align = "right"
          )
        ) %>%
        dplyr::ungroup()
    } else {
      log_warn("Missing required columns (customer or year) for registered products data")
    }
  }
  
  log_info("Added installed base features")
  return(panel_data)
}

#' Add temporal and relationship features to the panel
#'
#' @param panel_data Panel data
#' @return Panel with temporal and relationship features added
# add_temporal_features <- function(panel_data) {
#   log_info("Adding temporal and relationship features")
#   # log_info("Available columns for temporal features: %s", 
#   #          paste(head(sort(names(panel_data)), 20), collapse=", "))
#   # log_info("Checking for ts_first_explore_year: %s", 
#   #          "ts_first_explore_year" %in% names(panel_data))
#   # log_info("Checking for first_exploration_purchase_year: %s", 
#   #          "first_exploration_purchase_year" %in% names(panel_data))
#   # log_info("Adding temporal and relationship features")
#   
#   # Process temporal features
#   panel_data <- panel_data %>%
#     dplyr::arrange(customer_id, year) %>%
#     dplyr::group_by(customer_id) %>%
#     dplyr::mutate(
#       # Customer tenure calculation - IMPROVED VERSION
#       first_purchase_year = if(any(relationship_status == "active", na.rm = TRUE)) {
#         min(year[relationship_status == "active"], na.rm = TRUE)
#       } else {
#         NA_real_
#       },
#       
#       # Base customer tenure on first purchase (not first contact)
#       customer_tenure = ifelse(
#         relationship_status == "active" & !is.na(first_purchase_year),
#         year - first_purchase_year + 1,
#         0
#       ),
#       
#       # Calculate pre-purchase history length (how long they were prospects)
#       prospect_tenure = ifelse(
#         relationship_status == "pre_relationship" & !is.na(first_purchase_year),
#         first_purchase_year - year,
#         0
#       ),
#       
#       # First check which columns exist for treatment year calculation
#       has_sap_data = "ts_first_explore_year" %in% names(panel_data),
#       has_opp_data = "first_exploration_purchase_year" %in% names(panel_data),
#       
#       # Calculate treatment timeline variables using pre-checked columns
#       treatment_year = case_when(
#         # Use SAP data if available
#         has_sap_data & !is.na(ts_first_explore_year) ~ ts_first_explore_year,
#         # Use opportunities data as fallback
#         has_opp_data & !is.na(first_exploration_purchase_year) ~ first_exploration_purchase_year,
#         # Otherwise, set to NA
#         TRUE ~ NA_real_
#       ),
#       
#       # Calculate time since first purchase - critical for measuring lock-in
#       years_since_first_purchase = ifelse(
#         !is.na(first_purchase_year),
#         year - first_purchase_year,
#         NA_real_
#       ),
#       
#       # Calculate time-to-treatment (relative to first purchase)
#       time_to_treat = case_when(
#         is.na(treatment_year) ~ NA_real_,  # No treatment year, no time-to-treat
#         TRUE ~ year - treatment_year
#       ),
#       
#       # Calculate relationship length at treatment (for matching)
#       customer_relationship_length_at_treatment = ifelse(
#         time_to_treat == 0 & !is.na(customer_tenure), 
#         customer_tenure, 
#         NA_real_
#       )
#     )
#   
#   # Check if opportunity purchase metrics exist before calculating purchase metrics
#   if (any(c("to_sum_opps_won", "to_sum_opps_won_cum") %in% names(panel_data))) {
#     panel_data <- panel_data %>%
#       dplyr::mutate(
#         # Purchase dating and recency metrics
#         ft_purchase_at_t_date = case_when(
#           # First purchase based on won opportunities
#           "to_sum_opps_won_cum" %in% names(panel_data) && 
#             (lag(to_sum_opps_won_cum) == 0 | is.na(lag(to_sum_opps_won_cum))) & 
#             to_sum_opps_won_cum > 0 ~ year,
#           # First purchase based on registered products if they exist
#           "erp_registered_product_cum" %in% names(panel_data) && 
#             (lag(erp_registered_product_cum) == 0 | is.na(lag(erp_registered_product_cum))) & 
#             erp_registered_product_cum > 0 ~ year,
#           "Manual_Registered_Product_cum" %in% names(panel_data) && 
#             (lag(Manual_Registered_Product_cum) == 0 | is.na(lag(Manual_Registered_Product_cum))) & 
#             Manual_Registered_Product_cum > 0 ~ year,
#           TRUE ~ NA_real_
#         ),
#         
#         lt_purchase_at_T_date = ifelse("to_sum_opps_won" %in% names(panel_data) && 
#                                          to_sum_opps_won > 0, year, NA_real_)
#       )
#     
#     # Fill purchase date info down
#     panel_data <- panel_data %>%
#       tidyr::fill(c(ft_purchase_at_t_date, lt_purchase_at_T_date), .direction = "down")
#   }
#   
#   # Calculate software and relationship duration metrics if applicable
#   if ("ts_first_exploit_year" %in% names(panel_data)) {
#     panel_data <- panel_data %>%
#       dplyr::mutate(
#         ft_purchase_exploit = as.integer(!is.na(ts_first_exploit_year)),
#         exploit_duration_ft_purchase = ifelse(
#           !is.na(ts_first_exploit_year),
#           year - ts_first_exploit_year,
#           0
#         )
#       )
#   }
#   
#   if ("ts_first_explore_year" %in% names(panel_data)) {
#     panel_data <- panel_data %>%
#       dplyr::mutate(
#         ft_purchase_explore = as.integer(!is.na(ts_first_explore_year)),
#         explore_duration_ft_purchase = ifelse(
#           !is.na(ts_first_explore_year),
#           year - ts_first_explore_year,
#           0
#         )
#       )
#   }
#   
#   if ("ft_purchase_at_t_date" %in% names(panel_data)) {
#     panel_data <- panel_data %>%
#       dplyr::mutate(
#         customer_duration_ft_purchase = ifelse(
#           !is.na(ft_purchase_at_t_date),
#           year - ft_purchase_at_t_date,
#           0
#         )
#       )
#   }
#   
#   if ("lt_purchase_at_T_date" %in% names(panel_data)) {
#     panel_data <- panel_data %>%
#       dplyr::mutate(
#         duration_since_last_purchase = year - lag(lt_purchase_at_T_date)
#       )
#   }
#   
#   # Fill customer relationship length at treatment
#   panel_data <- panel_data %>%
#     tidyr::fill(customer_relationship_length_at_treatment, .direction = "downup") %>%
#     dplyr::ungroup()
#   
#   log_info("Added temporal features")
#   return(panel_data)
# }

add_temporal_features <- function(panel_data) {
  log_info("Adding temporal and relationship features")
  
  # Process temporal features
  panel_data <- panel_data %>%
    dplyr::arrange(customer_id, year) %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Customer tenure calculation
      first_purchase_year = if(any(relationship_status == "active", na.rm = TRUE)) {
        min(year[relationship_status == "active"], na.rm = TRUE)
      } else {
        NA_real_
      },
      
      # Base customer tenure on first purchase (not first contact)
      customer_tenure = ifelse(
        relationship_status == "active" & !is.na(first_purchase_year),
        year - first_purchase_year + 1,
        0
      ),
      
      # Calculate pre-purchase history length (how long they were prospects)
      prospect_tenure = ifelse(
        relationship_status == "pre_relationship" & !is.na(first_purchase_year),
        first_purchase_year - year,
        0
      ),
      
      # Calculate time since first purchase - critical for measuring lock-in
      years_since_first_purchase = ifelse(
        !is.na(first_purchase_year),
        year - first_purchase_year,
        NA_real_
      )
    )
  
  # Check if SAP software data variables exist and calculate time-to-treat
  if (all(c("ts_first_explore_year", "ts_years_since_explore") %in% names(panel_data))) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        # Calculate time-to-treatment based on SAP data
        time_to_treat = ifelse(
          !is.na(ts_first_explore_year), 
          ts_years_since_explore,
          NA_real_
        ),
        
        # Calculate relationship length at treatment (for matching)
        customer_relationship_length_at_treatment = ifelse(
          !is.na(time_to_treat) & time_to_treat == 0 & !is.na(customer_tenure), 
          customer_tenure, 
          NA_real_
        )
      )
  }
  
  # Calculate first purchase date - use separate checks to avoid case_when() issues
  panel_data <- panel_data %>%
    dplyr::mutate(
      # Initialize with NA
      ft_purchase_at_t_date = NA_real_,
      lt_purchase_at_T_date = NA_real_
    )
  
  # Add won opportunities first purchase if column exists
  if ("to_sum_opps_won_cum" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        ft_purchase_at_t_date = ifelse(
          (lag(to_sum_opps_won_cum, default = 0) == 0) & to_sum_opps_won_cum > 0,
          year,
          ft_purchase_at_t_date
        )
      )
  }
  
  # Add registered products first purchase if column exists
  if ("ERP_Registered_Product_cum" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        ft_purchase_at_t_date = ifelse(
          is.na(ft_purchase_at_t_date) & 
            (lag(ERP_Registered_Product_cum, default = 0) == 0) & 
            ERP_Registered_Product_cum > 0,
          year,
          ft_purchase_at_t_date
        )
      )
  }
  
  # Add manually registered products if column exists
  if ("Manual_Registered_Product_cum" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        ft_purchase_at_t_date = ifelse(
          is.na(ft_purchase_at_t_date) & 
            (lag(Manual_Registered_Product_cum, default = 0) == 0) & 
            Manual_Registered_Product_cum > 0,
          year,
          ft_purchase_at_t_date
        )
      )
  }
  
  # Add latest purchase date if column exists
  if ("to_sum_opps_won" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        lt_purchase_at_T_date = ifelse(
          to_sum_opps_won > 0,
          year,
          lt_purchase_at_T_date
        )
      )
  }
  
  # Fill purchase date info
  panel_data <- panel_data %>%
    tidyr::fill(c(ft_purchase_at_t_date, lt_purchase_at_T_date), .direction = "down")
  
  # Calculate software and relationship duration metrics if applicable
  if ("ts_first_exploit_year" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        exploit_duration_ft_purchase = ifelse(
          !is.na(ts_first_exploit_year),
          year - ts_first_exploit_year,
          0
        )
      )
  }
  
  if ("ts_first_explore_year" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        explore_duration_ft_purchase = ifelse(
          !is.na(ts_first_explore_year),
          year - ts_first_explore_year,
          0
        )
      )
  }
  
  # Calculate customer duration
  if ("ft_purchase_at_t_date" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      dplyr::mutate(
        customer_duration_ft_purchase = ifelse(
          !is.na(ft_purchase_at_t_date),
          year - ft_purchase_at_t_date,
          0
        )
      )
  }
  
  # Calculate time since last purchase - safely
  panel_data <- panel_data %>%
    dplyr::mutate(
      # Create a temporary variable for previous purchase year
      prev_purchase_year = dplyr::lag(lt_purchase_at_T_date),
      
      # Calculate duration since last purchase safely
      duration_since_last_purchase = ifelse(
        !is.na(prev_purchase_year),
        year - prev_purchase_year,
        NA_real_
      )
    ) %>%
    dplyr::select(-prev_purchase_year)  # Remove temporary column
  
  # Fill customer relationship length at treatment
  if ("customer_relationship_length_at_treatment" %in% names(panel_data)) {
    panel_data <- panel_data %>%
      tidyr::fill(customer_relationship_length_at_treatment, .direction = "downup")
  }
  
  panel_data <- panel_data %>%
    dplyr::ungroup()
  
  log_info("Added temporal features")
  return(panel_data)
}

#' Add treatment definition variables to the panel
#'
#' @param panel_data Panel data
#' @return Panel with treatment definition variables added
# add_treatment_variables <- function(panel_data) {
#   log_info("Adding treatment definition variables")
#   
#   # Check for required time_to_treat variable
#   if (!"time_to_treat" %in% names(panel_data)) {
#     log_error("Required variable 'time_to_treat' is missing - cannot create treatment variables")
#     # Create minimal treatment variables as fallback
#     panel_data$treatment <- 0
#     panel_data$time_since_treatment <- NA_real_
#     panel_data$post_period <- 0
#     panel_data$ever_treated <- 0
#     return(panel_data)
#   }
#   
#   # First check which columns exist (before mutate)
#   has_ts_post_explore <- "ts_post_explore_period" %in% names(panel_data)
#   has_post_explore <- "post_exploration_period" %in% names(panel_data)
#   has_ts_ever_purchased <- "ts_ever_purchased_explore" %in% names(panel_data)
#   has_ever_purchased <- "ever_purchased_exploration" %in% names(panel_data)
#   
#   # Log available columns for debugging
#   log_info("Treatment columns available - ts_post_explore_period: %s, post_exploration_period: %s",
#            has_ts_post_explore, has_post_explore)
#   
#   # Create treatment variables using safe column references
#   panel_data <- panel_data %>%
#     dplyr::mutate(
#       # Define the main treatment variable using pre-checked column existence
#       treatment = case_when(
#         has_ts_post_explore & !is.na(ts_post_explore_period) & ts_post_explore_period == 1 ~ 1,
#         has_post_explore & !is.na(post_exploration_period) & post_exploration_period == 1 ~ 1,
#         TRUE ~ 0
#       ),
#       
#       # Track time since treatment
#       time_since_treatment = ifelse(treatment == 1, time_to_treat, NA_real_),
#       
#       # Define pre/post periods for diff-in-diff analysis
#       post_period = ifelse(time_to_treat >= 0, 1, 0),
#       
#       # Define pre/post periods with leads and lags
#       pre_treatment_1 = ifelse(time_to_treat == -1, 1, 0),
#       pre_treatment_2 = ifelse(time_to_treat == -2, 1, 0),
#       pre_treatment_3plus = ifelse(time_to_treat <= -3, 1, 0),
#       
#       post_treatment_0 = ifelse(time_to_treat == 0, 1, 0),
#       post_treatment_1 = ifelse(time_to_treat == 1, 1, 0),
#       post_treatment_2 = ifelse(time_to_treat == 2, 1, 0),
#       post_treatment_3plus = ifelse(time_to_treat >= 3, 1, 0),
#       
#       # Define ever treated indicator using pre-checked column existence
#       ever_treated = case_when(
#         has_ts_ever_purchased & !is.na(ts_ever_purchased_explore) & ts_ever_purchased_explore ~ 1,
#         has_ever_purchased & !is.na(ever_purchased_exploration) & ever_purchased_exploration ~ 1,
#         TRUE ~ 0
#       )
#     )
#   
#   # Verify successful creation of key variables
#   treatment_created <- "treatment" %in% names(panel_data)
#   log_info("Treatment variables created successfully: %s", treatment_created)
#   
#   if (treatment_created) {
#     log_info("Treatment summary: %d treated observations out of %d total", 
#              sum(panel_data$treatment, na.rm = TRUE), nrow(panel_data))
#   }
#   
#   return(panel_data)
# }
add_treatment_variables <- function(panel_data) {
  log_info("Adding treatment definition variables")
  
  # Check for required variables
  required_vars <- c("ts_years_since_explore", "customer_tenure", "ts_first_explore_year", "ts_post_explore_period")
  missing_vars <- setdiff(required_vars, names(panel_data))
  
  if (length(missing_vars) > 0) {
    log_warn("Required variables missing for treatment definition: %s", paste(missing_vars, collapse=", "))
    # Create fallback variables if necessary
    if (!"ts_post_explore_period" %in% names(panel_data) && "ts_first_explore_year" %in% names(panel_data)) {
      panel_data <- panel_data %>%
        dplyr::mutate(
          ts_post_explore_period = ifelse(
            !is.na(ts_first_explore_year) & year >= ts_first_explore_year,
            1, 0
          )
        )
      log_info("Created fallback ts_post_explore_period variable")
    }
  }
  
  # Define treatment variables
  panel_data <- panel_data %>%
    dplyr::group_by(customer_id) %>%
    dplyr::mutate(
      # Define main treatment variable - exploration software purchase
      # This replicates the logic from the engineering.R script
      treatment = ifelse(ts_post_explore_period == 1, 1, 0),
      
      # Calculate time-to-treat using the same logic as engineering.R
      time_to_treat = case_when(
        !is.na(ts_first_explore_year) ~ ts_years_since_explore,
        year == 2014 ~ 0,
        TRUE ~ year - 2014
      ),
      
      # Define pre/post periods for diff-in-diff analysis
      post_period = ifelse(time_to_treat >= 0, 1, 0),
      
      # Treatment indicator - any customer who ever purchased exploration software
      ever_treated = ifelse(!is.na(ts_first_explore_year), 1, 0),
      
      # Create period indicators for event studies
      pre_treatment_1 = ifelse(time_to_treat == -1, 1, 0),
      pre_treatment_2 = ifelse(time_to_treat == -2, 1, 0),
      pre_treatment_3plus = ifelse(time_to_treat <= -3, 1, 0),
      
      post_treatment_0 = ifelse(time_to_treat == 0, 1, 0),
      post_treatment_1 = ifelse(time_to_treat == 1, 1, 0),
      post_treatment_2 = ifelse(time_to_treat == 2, 1, 0),
      post_treatment_3plus = ifelse(time_to_treat >= 3, 1, 0),
      
      # For matching - capture relationship length at treatment time
      customer_relationship_length_at_treatment = ifelse(
        time_to_treat == 0 & !is.na(customer_tenure),
        customer_tenure,
        NA_real_
      )
    )
  
  # Fill relationship length at treatment time
  panel_data <- panel_data %>%
    tidyr::fill(customer_relationship_length_at_treatment, .direction = "downup") %>%
    dplyr::ungroup()
  
  # Log treatment statistics
  treated_customers <- sum(panel_data$ever_treated, na.rm = TRUE)
  total_customers <- n_distinct(panel_data$customer_id)
  log_info("Treatment summary: %d treated customers (%.1f%%) out of %d total", 
           treated_customers, 
           treated_customers/total_customers*100,
           total_customers)
  
  return(panel_data)
}

#' Clean and finalize the panel dataset
#'
#' @param panel_data Panel data
#' @param min_year Minimum year to include
#' @param max_year Maximum year to include
#' @param country_filter Vector of countries to include
#' @return Cleaned and filtered panel dataset
# clean_panel_dataset <- function(panel_data, min_year = 2007, max_year = 2019,
#                                 country_filter = "Germany") {
#   log_info("Cleaning and finalizing panel dataset")
#   
#   # Identify suspicious customers (copied from original script)
#   outliers_and_suspicious_customers <- panel_data
#   
#   # Only filter based on to_sum_val_opps_won if it exists
#   if ("to_sum_val_opps_won" %in% names(panel_data)) {
#     outliers_and_suspicious_customers <- panel_data %>%
#       dplyr::filter(
#         (to_sum_val_opps_won > 0 & to_sum_val_opps_won <= 10) |
#           (country == country_filter & to_sum_val_opps_won > 10000000) |
#           grepl("Dummy|TRUMPF", customer_name, ignore.case = TRUE)
#       )
#   } else {
#     outliers_and_suspicious_customers <- panel_data %>%
#       dplyr::filter(
#         grepl("Dummy|TRUMPF", customer_name, ignore.case = TRUE)
#       )
#   }
#   
#   # Remove outliers and suspicious customers
#   cleaned_panel <- panel_data %>%
#     dplyr::anti_join(
#       outliers_and_suspicious_customers,
#       by = "customer_id"
#     ) %>%
#     # Apply year and country filters
#     dplyr::filter(
#       year >= min_year & year <= max_year
#     )
#   
#   # Apply country filter if the column exists
#   if ("country" %in% names(cleaned_panel)) {
#     cleaned_panel <- cleaned_panel %>%
#       dplyr::filter(is.na(country) | country == country_filter)
#   }
#   
#   # Replace remaining NA values with 0 for numeric columns in active relationship periods
#   cleaned_panel <- cleaned_panel %>%
#     dplyr::mutate(
#       across(
#         where(is.numeric),
#         ~ifelse(relationship_status == "active" & is.na(.), 0, .)
#       )
#     )
#   
#   log_info("Panel cleaned: %d customers, %d years, %d rows", 
#            n_distinct(cleaned_panel$customer_id),
#            n_distinct(cleaned_panel$year),
#            nrow(cleaned_panel))
#   
#   return(cleaned_panel)
# }

clean_panel_dataset <- function(panel_data, min_year = 2007, max_year = 2019,
                                country_filter = "Germany") {
  log_info("Cleaning and finalizing panel dataset")
  
  # Identify suspicious customers (improved version)
  outliers_and_suspicious_customers <- panel_data %>%
    dplyr::filter(
      # Unreasonably small purchases
      (to_sum_val_opps_won > 0 & to_sum_val_opps_won <= 10) |
        # Extremely large purchases in filtered country
        (country == country_filter & to_sum_val_opps_won > 10000000) |
        # Internal or test accounts
        grepl("Dummy|TRUMPF|Test|Demo", customer_name, ignore.case = TRUE) |
        # Add: Customers with no actual purchases
        (is.na(first_purchase_year) & relationship_status != "pre_relationship")
    )
  
  # Remove outliers and suspicious customers
  cleaned_panel <- panel_data %>%
    dplyr::anti_join(
      outliers_and_suspicious_customers,
      by = "customer_id"
    ) %>%
    # Apply year filter
    dplyr::filter(
      year >= min_year & year <= max_year
    )
  
  # Apply country filter if the column exists
  if ("country" %in% names(cleaned_panel)) {
    cleaned_panel <- cleaned_panel %>%
      dplyr::filter(is.na(country) | country == country_filter)
  }
  
  # Keep only real customers with actual purchase history for lock-in analysis
  cleaned_panel <- cleaned_panel %>%
    dplyr::filter(!is.na(first_purchase_year))
  
  # Replace remaining NA values with 0 for numeric columns in active relationship periods
  cleaned_panel <- cleaned_panel %>%
    dplyr::mutate(
      across(
        where(is.numeric),
        ~ifelse(relationship_status == "active" & is.na(.), 0, .)
      )
    )
  
  log_info("Panel cleaned: %d customers, %d years, %d rows", 
           n_distinct(cleaned_panel$customer_id),
           n_distinct(cleaned_panel$year),
           nrow(cleaned_panel))
  
  return(cleaned_panel)
}

# Main function to build complete panel -------------------------------------

#' Build a complete panel dataset for quasi-experimental analysis
#'
#' @param use_checkpoints Whether to use checkpoints (default: TRUE)
#' @param min_year Minimum year to include (default: 2007)
#' @param max_year Maximum year to include (default: 2019)
#' @param country_filter Vector of countries to include (default: "Germany")
#' @return Complete panel dataset ready for analysis
# build_panel_dataset <- function(use_checkpoints = TRUE, min_year = 2007, 
#                                 max_year = 2019, country_filter = "Germany") {
#   log_info("Starting panel dataset construction for %d-%d", min_year, max_year)
#   
#   # 1. Load all necessary data sources
#   log_info("Loading data sources")
#   all_data <- load_processed_data()
#   product_categories <- load_product_categories()
#   
#   if (is.null(all_data$opps_data)) {
#     log_error("Required dataset 'opps_data' not available")
#     return(NULL)
#   }
#   
#   # 2. Create base panel structure
#   # if (use_checkpoints) {
#   #   panel_data <- load_checkpoint("base_panel")
#   # }
#   # 
#   # if (is.null(panel_data)) {
#   #   # Create the base panel
#   #   tryCatch({
#   #     panel_data <- create_base_panel(all_data$opps_data, 
#   #                                     all_data$customer_data,
#   #                                     year_range = c(min_year - 5, max_year))
#   #     if (use_checkpoints) save_checkpoint(panel_data, "base_panel")
#   #   }, error = function(e) {
#   #     log_error("Error creating base panel: %s", e$message)
#   #     stop(paste("Failed to create base panel:", e$message))
#   #   })
#   # }
#   
#   # 2. Create base panel structure
#   if (use_checkpoints) {
#     panel_data <- load_checkpoint("base_panel")
#   }
#   
#   if (is.null(panel_data)) {
#     # Create the base panel
#     tryCatch({
#       # FIXED: Removed the customer_data argument
#       panel_data <- create_base_panel(all_data$opps_data, 
#                                       year_range = c(min_year - 5, max_year)) ## Why are we substracting 5 here?
#       if (use_checkpoints) save_checkpoint(panel_data, "base_panel")
#     }, error = function(e) {
#       log_error("Error creating base panel: %s", e$message)
#       stop(paste("Failed to create base panel:", e$message))
#     })
#   }
#   
#   # 3. Add customer characteristics
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("customer_chars")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded customer characteristics from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_customer_characteristics(
#           panel_data, all_data$opps_data, all_data$customer_data
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "customer_chars")
#       }, error = function(e) {
#         log_error("Error adding customer characteristics: %s", e$message)
#         # Provide debug information
#         log_error("Columns in opportunities data: %s", 
#                   paste(names(all_data$opps_data), collapse=", "))
#         stop(paste("Failed to add customer characteristics:", e$message))
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_customer_characteristics(
#         panel_data, all_data$opps_data, all_data$customer_data
#       )
#     }, error = function(e) {
#       log_error("Error adding customer characteristics: %s", e$message)
#       stop(paste("Failed to add customer characteristics:", e$message))
#     })
#   }
#   
#   # 4. Add opportunity features
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("opportunity_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded opportunity features from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_opportunity_features(
#           panel_data, all_data$opps_data, product_categories
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "opportunity_features")
#       }, error = function(e) {
#         log_error("Error adding opportunity features: %s", e$message)
#         stop(paste("Failed to add opportunity features:", e$message))
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_opportunity_features(
#         panel_data, all_data$opps_data, product_categories
#       )
#     }, error = function(e) {
#       log_error("Error adding opportunity features: %s", e$message)
#       stop(paste("Failed to add opportunity features:", e$message))
#     })
#   }
#   
#   # 5. Add software features
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("software_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded software features from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_software_features(
#           panel_data, all_data$sap_software, all_data$customer_data
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "software_features")
#       }, error = function(e) {
#         log_error("Error adding software features: %s", e$message)
#         stop(paste("Failed to add software features:", e$message))
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_software_features(
#         panel_data, all_data$sap_software, all_data$customer_data
#       )
#     }, error = function(e) {
#       log_error("Error adding software features: %s", e$message)
#       stop(paste("Failed to add software features:", e$message))
#     })
#   }
#   
#   # 6. Add interaction features - with better error handling
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("interaction_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded interaction features from checkpoint")
#     } else {
#       tryCatch({
#         # Try with all data first
#         log_info("Adding interaction features")
#         panel_data <- add_interaction_features(
#           panel_data, all_data$meetings_data, all_data$mails_data, all_data$other_act_data
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "interaction_features")
#       }, error = function(e) {
#         log_error("Error adding interaction features: %s", e$message)
#         if (use_checkpoints) save_checkpoint(panel_data, "interaction_features_error")
#         
#         # Try without other activities data
#         log_warn("Trying to continue without other_act_data")
#         tryCatch({
#           panel_data <- add_interaction_features(
#             panel_data, all_data$meetings_data, all_data$mails_data, NULL
#           )
#           if (use_checkpoints) save_checkpoint(panel_data, "interaction_features_partial")
#         }, error = function(e2) {
#           log_error("Still failed to add interaction features: %s", e2$message)
#           
#           # Try with completely minimal interaction features
#           log_warn("Creating minimal interaction features as fallback")
#           panel_data$completed_meetings_num_total <- 0
#           panel_data$mails_total <- 0
#           panel_data$other_act_phone_calls <- 0
#           panel_data$other_act_letters <- 0
#           panel_data$other_act_faxes <- 0
#           panel_data$sales_act_synchronous <- 0
#           panel_data$sales_act_asynchronous <- 0
#           panel_data$aggregated_sales_act <- 0
#           panel_data$effort_to_revenue_ratio <- 0
#           panel_data$lagged_effort_to_revenue_ratio <- 0
#           panel_data$rolling_interactions_3yr <- NA_real_
#           panel_data$rolling_efforts_3yr <- NA_real_
#           
#           log_info("Created minimal interaction features")
#           if (use_checkpoints) save_checkpoint(panel_data, "interaction_features_minimal")
#           
#           # Don't stop the process, continue with minimal features
#         })
#       })
#     }
#   } else {
#     # No checkpoints, but still use error handling
#     tryCatch({
#       panel_data <- add_interaction_features(
#         panel_data, all_data$meetings_data, all_data$mails_data, all_data$other_act_data
#       )
#     }, error = function(e) {
#       log_error("Error adding interaction features: %s", e$message)
#       
#       # Try without other activities data
#       log_warn("Trying to continue without other_act_data")
#       tryCatch({
#         panel_data <- add_interaction_features(
#           panel_data, all_data$meetings_data, all_data$mails_data, NULL
#         )
#       }, error = function(e2) {
#         log_error("Still failed to add interaction features: %s", e2$message)
#         
#         # Create minimal interaction features as fallback
#         log_warn("Creating minimal interaction features as fallback")
#         panel_data$completed_meetings_num_total <- 0
#         panel_data$mails_total <- 0
#         panel_data$other_act_phone_calls <- 0
#         panel_data$other_act_letters <- 0
#         panel_data$other_act_faxes <- 0
#         panel_data$sales_act_synchronous <- 0
#         panel_data$sales_act_asynchronous <- 0
#         panel_data$aggregated_sales_act <- 0
#         panel_data$effort_to_revenue_ratio <- 0
#         panel_data$lagged_effort_to_revenue_ratio <- 0
#         panel_data$rolling_interactions_3yr <- NA_real_
#         panel_data$rolling_efforts_3yr <- NA_real_
#         
#         log_info("Created minimal interaction features")
#       })
#     })
#   }
#   
#   # 7. Add service features
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("service_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded service features from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_service_features(
#           panel_data, all_data$sis_cases, all_data$sis_missions, all_data$customer_data
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "service_features")
#       }, error = function(e) {
#         log_error("Error adding service features: %s", e$message)
#         # Continue with process, service features are not critical
#         log_warn("Continuing without service features")
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_service_features(
#         panel_data, all_data$sis_cases, all_data$sis_missions, all_data$customer_data
#       )
#     }, error = function(e) {
#       log_error("Error adding service features: %s", e$message)
#       # Continue with process
#       log_warn("Continuing without service features")
#     })
#   }
#   
#   # 8. Add installed base features
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("installed_base_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded installed base features from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_installed_base_features(
#           panel_data, all_data$sis_installed_base, all_data$reg_products, all_data$customer_data
#         )
#         if (use_checkpoints) save_checkpoint(panel_data, "installed_base_features")
#       }, error = function(e) {
#         log_error("Error adding installed base features: %s", e$message)
#         # Continue with process
#         log_warn("Continuing without installed base features")
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_installed_base_features(
#         panel_data, all_data$sis_installed_base, all_data$reg_products, all_data$customer_data
#       )
#     }, error = function(e) {
#       log_error("Error adding installed base features: %s", e$message)
#       # Continue with process
#       log_warn("Continuing without installed base features")
#     })
#   }
#   
#   # 9. Add temporal features
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("temporal_features")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded temporal features from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_temporal_features(panel_data)
#         if (use_checkpoints) save_checkpoint(panel_data, "temporal_features")
#       }, error = function(e) {
#         log_error("Error adding temporal features: %s", e$message)
#         # This is more critical, but continue with basic temporal features
#         log_warn("Adding minimal temporal features as fallback")
#         panel_data <- panel_data %>%
#           dplyr::group_by(customer_id) %>%
#           dplyr::mutate(
#             customer_tenure = 1,
#             time_to_treat = 0
#           ) %>%
#           dplyr::ungroup()
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_temporal_features(panel_data)
#     }, error = function(e) {
#       log_error("Error adding temporal features: %s", e$message)
#       # Add minimal temporal features
#       log_warn("Adding minimal temporal features as fallback")
#       panel_data <- panel_data %>%
#         dplyr::group_by(customer_id) %>%
#         dplyr::mutate(
#           customer_tenure = 1,
#           time_to_treat = 0
#         ) %>%
#         dplyr::ungroup()
#     })
#   }
#   
#   # 10. Add treatment variables
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint("treatment_variables")
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded treatment variables from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- add_treatment_variables(panel_data)
#         if (use_checkpoints) save_checkpoint(panel_data, "treatment_variables")
#       }, error = function(e) {
#         log_error("Error adding treatment variables: %s", e$message)
#         # This is critical, add minimal treatment variables
#         log_warn("Adding minimal treatment variables as fallback")
#         panel_data$treatment <- 0
#         panel_data$post_period <- 0
#         panel_data$ever_treated <- 0
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- add_treatment_variables(panel_data)
#     }, error = function(e) {
#       log_error("Error adding treatment variables: %s", e$message)
#       # Add minimal treatment variables
#       log_warn("Adding minimal treatment variables as fallback")
#       panel_data$treatment <- 0
#       panel_data$post_period <- 0
#       panel_data$ever_treated <- 0
#     })
#   }
#   
#   # 11. Clean and finalize the panel
#   if (use_checkpoints) {
#     checkpoint_data <- load_checkpoint(paste0("final_panel_", min_year, "_", max_year))
#     if (!is.null(checkpoint_data)) {
#       panel_data <- checkpoint_data
#       log_info("Loaded final panel from checkpoint")
#     } else {
#       tryCatch({
#         panel_data <- clean_panel_dataset(
#           panel_data, min_year = min_year, max_year = max_year, country_filter = country_filter
#         )
#         attr(panel_data, "cleaned") <- TRUE
#         if (use_checkpoints) {
#           save_checkpoint(panel_data, paste0("final_panel_", min_year, "_", max_year))
#         }
#       }, error = function(e) {
#         log_error("Error cleaning panel dataset: %s", e$message)
#         # Skip cleaning but still return what we have
#         log_warn("Returning uncleaned panel")
#         attr(panel_data, "cleaned") <- FALSE
#       })
#     }
#   } else {
#     tryCatch({
#       panel_data <- clean_panel_dataset(
#         panel_data, min_year = min_year, max_year = max_year, country_filter = country_filter
#       )
#       attr(panel_data, "cleaned") <- TRUE
#     }, error = function(e) {
#       log_error("Error cleaning panel dataset: %s", e$message)
#       # Skip cleaning but still return what we have
#       log_warn("Returning uncleaned panel")
#       attr(panel_data, "cleaned") <- FALSE
#     })
#   }
#   
#   # Add metadata
#   attr(panel_data, "creation_date") <- Sys.Date()
#   attr(panel_data, "year_range") <- c(min_year, max_year)
#   attr(panel_data, "countries") <- country_filter
#   attr(panel_data, "n_customers") <- n_distinct(panel_data$customer_id)
#   
#   # Calculate number of treated safely
#   if ("ever_treated" %in% names(panel_data)) {
#     attr(panel_data, "n_treated") <- sum(unique(panel_data$ever_treated), na.rm = TRUE)
#   } else {
#     attr(panel_data, "n_treated") <- 0
#   }
#   
#   log_info("Panel dataset construction complete: %d rows, %d customers, years %d-%d",
#            nrow(panel_data), attr(panel_data, "n_customers"), min_year, max_year)
#   
#   return(panel_data)
# }

build_panel_dataset <- function(use_checkpoints = TRUE, min_year = 2007, 
                                max_year = 2019, country_filter = "Germany") {
  log_info("Starting panel dataset construction for %d-%d", min_year, max_year)
  
  # Load data
  all_data <- load_processed_data()
  product_categories <- load_product_categories()
  
  # Create base panel
  panel_data <- create_base_panel(all_data$opps_data, 
                                  year_range = c(min_year - 5, max_year))
  
  # Add features in the correct order
  panel_data <- add_customer_characteristics(
    panel_data, all_data$opps_data, all_data$customer_data)
  
  panel_data <- add_opportunity_features(
    panel_data, all_data$opps_data, product_categories)
  
  # Critical step: add software features using SAP data
  panel_data <- add_software_features(
    panel_data, all_data$sap_software, all_data$customer_data)
  
  # Add other features
  panel_data <- add_interaction_features(
    panel_data, all_data$meetings_data, all_data$mails_data, all_data$other_act_data)
  
  panel_data <- add_service_features(
    panel_data, all_data$sis_cases, all_data$sis_missions, all_data$customer_data)
  
  panel_data <- add_installed_base_features(
    panel_data, all_data$sis_installed_base, all_data$reg_products, all_data$customer_data)
  
  panel_data <- add_temporal_features(panel_data)
  
  # Critical step: create treatment variables correctly
  panel_data <- add_treatment_variables(panel_data)
  
  # Clean and filter
  panel_data <- clean_panel_dataset(
    panel_data, min_year = min_year, max_year = max_year, country_filter = country_filter)
  
  return(panel_data)
}

# Save the final panel dataset ----------------------------------------------

#' Save the panel dataset to disk
#'
#' @param panel_data Panel dataset
#' @param suffix Optional suffix for the filename
#' @return Path to the saved file
save_panel_dataset <- function(panel_data, suffix = NULL) {
  current_date <- format(Sys.Date(), "%Y%m%d")
  
  if (is.null(suffix)) {
    year_range <- attr(panel_data, "year_range")
    if (!is.null(year_range)) {
      suffix <- paste0(year_range[1], "_", year_range[2])
    } else {
      suffix <- "full"
    }
  }
  
  filename <- paste0("panel_dataset_", current_date, "_", suffix, ".rds")
  filepath <- here::here("data", "final", filename)
  
  saveRDS(panel_data, filepath)
  log_info("Panel dataset saved to %s", filepath)
  
  # Save metadata separately
  metadata <- list(
    creation_date = attr(panel_data, "creation_date"),
    year_range = attr(panel_data, "year_range"),
    countries = attr(panel_data, "countries"),
    n_customers = attr(panel_data, "n_customers"),
    n_treated = attr(panel_data, "n_treated"),
    n_rows = nrow(panel_data),
    n_cols = ncol(panel_data),
    column_names = names(panel_data)
  )
  
  metadata_file <- gsub("\\.rds$", "_metadata.rds", filepath)
  saveRDS(metadata, metadata_file)
  
  # Also save a summary to a text file for easy reference
  summary_file <- gsub("\\.rds$", "_summary.txt", filepath)
  sink(summary_file)
  cat("PANEL DATASET SUMMARY\n")
  cat("====================\n\n")
  cat(sprintf("Creation date: %s\n", attr(panel_data, "creation_date")))
  cat(sprintf("Year range: %d-%d\n", attr(panel_data, "year_range")[1], attr(panel_data, "year_range")[2]))
  cat(sprintf("Countries: %s\n", paste(attr(panel_data, "countries"), collapse = ", ")))
  cat(sprintf("Number of customers: %d\n", attr(panel_data, "n_customers")))
  cat(sprintf("Number of customers treated (exploration SW): %d\n", attr(panel_data, "n_treated")))
  cat(sprintf("Number of rows: %d\n", nrow(panel_data)))
  cat(sprintf("Number of columns: %d\n", ncol(panel_data)))
  
  # Calculate summary statistics for key variables
  cat("\nSummary statistics for key variables:\n")
  key_vars <- intersect(c(
    "to_sum_val_opps_won", "to_number_of_opps", "customer_tenure",
    "sales_act_synchronous", "sales_act_asynchronous", "treatment",
    "time_to_treat", "time_since_treatment"
  ), names(panel_data))
  
  for (var in key_vars) {
    if (is.numeric(panel_data[[var]])) {
      cat(sprintf("\n%s:\n", var))
      cat(sprintf("  Mean: %.2f\n", mean(panel_data[[var]], na.rm = TRUE)))
      cat(sprintf("  Median: %.2f\n", median(panel_data[[var]], na.rm = TRUE)))
      cat(sprintf("  Min: %.2f\n", min(panel_data[[var]], na.rm = TRUE)))
      cat(sprintf("  Max: %.2f\n", max(panel_data[[var]], na.rm = TRUE)))
      cat(sprintf("  NA values: %d (%.1f%%)\n", 
                  sum(is.na(panel_data[[var]])),
                  sum(is.na(panel_data[[var]])) / nrow(panel_data) * 100))
    } else if (is.factor(panel_data[[var]]) || is.character(panel_data[[var]]) || is.logical(panel_data[[var]])) {
      cat(sprintf("\n%s:\n", var))
      tab <- table(panel_data[[var]], useNA = "ifany")
      for (i in 1:length(tab)) {
        cat(sprintf("  %s: %d (%.1f%%)\n", 
                    names(tab)[i], 
                    tab[i],
                    tab[i] / sum(tab) * 100))
      }
    }
  }
  
  cat("\nColumn names:\n")
  cat(paste(names(panel_data), collapse = ", "))
  cat("\n\n")
  sink()
  
  log_info("Panel dataset summary saved to %s", summary_file)
  
  return(filepath)
}

# Additional helper function to explore the panel data ---------------------

#' Explore the panel dataset with summary statistics
#'
#' @param panel_data Panel dataset
#' @param group_vars Variables to group by (default: treatment)
#' @param metrics Variables to calculate statistics for
#' @return A data frame with summary statistics
explore_panel <- function(panel_data, group_vars = "treatment", 
                          metrics = NULL) {
  log_info("Exploring panel data with summary statistics")
  
  # If no metrics specified, use common ones that might be available
  if (is.null(metrics)) {
    metrics <- intersect(c(
      "to_sum_val_opps_won", "to_number_of_opps", "to_opp_winning_probability",
      "customer_tenure", "sales_act_synchronous", "sales_act_asynchronous",
      "n_cases", "service_case_intensity", "installed_hw_power_bi",
      "ts_sales_total_sw", "ts_sales_total_exploration", "ts_sales_total_exploitation"
    ), names(panel_data))
    
    if (length(metrics) == 0) {
      log_warn("No valid metrics found in the panel data")
      return(NULL)
    }
    
    log_info("Using metrics: %s", paste(metrics, collapse=", "))
  }
  
  # Create the summary
  summary_data <- panel_data %>%
    dplyr::group_by(across(all_of(group_vars))) %>%
    dplyr::summarise(
      n_observations = n(),
      n_customers = n_distinct(customer_id),
      across(
        all_of(metrics),
        list(
          mean = ~mean(., na.rm = TRUE),
          median = ~median(., na.rm = TRUE),
          sd = ~sd(., na.rm = TRUE),
          min = ~min(., na.rm = TRUE),
          max = ~max(., na.rm = TRUE)
        ),
        .names = "{.col}_{.fn}"
      ),
      .groups = "drop"
    )
  
  return(summary_data)
}

#' Visualize treatment effects in the panel data
#'
#' @param panel_data Panel dataset
#' @param outcome_var Outcome variable to visualize
#' @param time_var Time variable for x-axis (default: time_to_treat)
#' @param group_var Variable for grouping (default: treatment)
#' @param return_plot Whether to return the plot object (default: TRUE)
#' @param save_plot Whether to save the plot to disk (default: FALSE)
#' @param save_dir Directory to save plots to (default: "plots")
#' @return A ggplot2 object if return_plot is TRUE
visualize_treatment <- function(panel_data, outcome_var, 
                                time_var = "time_to_treat", 
                                group_var = "treatment",
                                return_plot = TRUE,
                                save_plot = FALSE,
                                save_dir = "plots") {
  
  # Check if required variables exist
  if (!all(c(outcome_var, time_var, group_var) %in% names(panel_data))) {
    missing_vars <- setdiff(c(outcome_var, time_var, group_var), names(panel_data))
    log_error("Required variables missing: %s", paste(missing_vars, collapse=", "))
    return(NULL)
  }
  
  # Create time-aggregated dataset
  agg_data <- panel_data %>%
    dplyr::group_by(.data[[time_var]], .data[[group_var]]) %>%
    dplyr::summarise(
      outcome_mean = mean(.data[[outcome_var]], na.rm = TRUE),
      outcome_se = sd(.data[[outcome_var]], na.rm = TRUE) / sqrt(n()),
      n = n(),
      .groups = "drop"
    )
  
  # Create the plot
  plot_title <- paste("Effect of Treatment on", gsub("_", " ", outcome_var))
  
  p <- ggplot2::ggplot(agg_data, ggplot2::aes(
    x = .data[[time_var]], 
    y = outcome_mean,
    color = factor(.data[[group_var]]),
    group = factor(.data[[group_var]])
  )) +
    ggplot2::geom_line() +
    ggplot2::geom_point() +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = outcome_mean - 1.96 * outcome_se, 
                   ymax = outcome_mean + 1.96 * outcome_se),
      width = 0.2
    ) +
    ggplot2::geom_vline(xintercept = 0, linetype = "dashed") +
    ggplot2::labs(
      title = plot_title,
      x = gsub("_", " ", time_var),
      y = gsub("_", " ", outcome_var),
      color = gsub("_", " ", group_var)
    ) +
    ggplot2::theme_minimal()
  
  # Save plot if requested
  if (save_plot) {
    if (!dir.exists(save_dir)) {
      dir.create(save_dir, recursive = TRUE)
    }
    
    filename <- paste0(
      "treatment_effect_", 
      gsub("_", "-", outcome_var), 
      "_", 
      format(Sys.Date(), "%Y%m%d"), 
      ".png"
    )
    filepath <- file.path(save_dir, filename)
    
    ggplot2::ggsave(filepath, p, width = 10, height = 6)
    log_info("Plot saved to %s", filepath)
  }
  
  # Return plot if requested
  if (return_plot) {
    return(p)
  } else {
    return(invisible(NULL))
  }
}

# Run the panel creation process -------------------------------------------
if (sys.nframe() == 0) {
  # This means the script is being run directly, not sourced
  set.seed(123) # For reproducibility
  start_time <- Sys.time()
  
  tryCatch({
    # Build panel dataset with checkpoints
    panel_data <- build_panel_dataset(
      use_checkpoints = TRUE,
      min_year = 1990,
      max_year = 2020,
      country_filter = "Germany"
    )
    
    # Save the final result
    if (!is.null(panel_data)) {
      save_panel_dataset(panel_data)
      
      # Create some exploratory plots
      dir.create(here::here("plots"), showWarnings = FALSE)
      
      outcome_vars <- intersect(c(
        "to_sum_val_opps_won", "to_number_of_opps", "to_opp_winning_probability",
        "sales_act_synchronous", "sales_act_asynchronous", 
        "ts_sales_total_exploration", "ts_sales_total_exploitation"
      ), names(panel_data))
      
      for (outcome in outcome_vars) {
        tryCatch({
          visualize_treatment(
            panel_data, 
            outcome_var = outcome,
            save_plot = TRUE,
            save_dir = here::here("plots")
          )
        }, error = function(e) {
          log_warn("Failed to create plot for %s: %s", outcome, e$message)
        })
      }
    }
    
    end_time <- Sys.time()
    total_time <- difftime(end_time, start_time, units = "mins")
    log_info("Panel creation process completed in %.2f minutes", total_time)
    
  }, error = function(e) {
    log_error("Error in panel creation process: %s", e$message)
    message("Process failed. Check the log file for details: ", log_file)
  })
}
