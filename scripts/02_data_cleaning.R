# 02_data_cleaning.R
# Purpose: Validate, clean, and transform loaded data for exploratory analysis

# Setup -------------------------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  here,
  tidyverse,
  lubridate,
  data.table
  )

# -------------------------------------------------------------------------
# Integrity Checking
# -------------------------------------------------------------------------
check_integrity <- function(df, df_name, expected_cols = NULL) {
  cat("------------------------------------------------------------\n")
  cat("Integrity check for:", df_name, "\n")
  if (is.null(df)) {
    cat("Data is NULL. Skipping.\n\n")
    return(invisible(NULL))
  }
  cat("Dimensions:", paste(dim(df), collapse = " x "), "\n")
  if (!is.null(expected_cols)) {
    missing_cols <- setdiff(expected_cols, names(df))
    if (length(missing_cols) > 0) {
      warning("Missing columns in ", df_name, ": ", paste(missing_cols, collapse = ", "))
    } else {
      cat("All expected columns are present in", df_name, "\n")
    }
  }
  cat("Summary of", df_name, ":\n")
  print(summary(df))
  cat("------------------------------------------------------------\n\n")
}

# A safe way to read RDS files (warn if missing)
safe_readRDS <- function(path) {
  if (file.exists(path)) {
    readRDS(path)
  } else {
    warning("File not found: ", path)
    return(NULL)
  }
}

# -------------------------------------------------------------------------
# Load All Intermediate .rds Files
# (matching 01_data_loading.R outputs)
# -------------------------------------------------------------------------
opps_data            <- safe_readRDS(here::here("data", "interim", "opps_data.rds"))
quotes_data          <- safe_readRDS(here::here("data", "interim", "quotes_data.rds"))
sap_software_data    <- safe_readRDS(here::here("data", "interim", "sap_software.rds"))
sap_machines_data    <- safe_readRDS(here::here("data", "interim", "sap_machines.rds"))
sis_installed_base   <- safe_readRDS(here::here("data", "interim", "sis_installed_base.rds"))
sis_cases            <- safe_readRDS(here::here("data", "interim", "sis_cases.rds"))
sis_missions         <- safe_readRDS(here::here("data", "interim", "sis_missions.rds"))
customer_data        <- safe_readRDS(here::here("data", "interim", "customer_data.rds"))
contacts_data        <- safe_readRDS(here::here("data", "interim", "contacts_data.rds"))
meetings_data        <- safe_readRDS(here::here("data", "interim", "meetings_data.rds"))
mails_data           <- safe_readRDS(here::here("data", "interim", "mails_data.rds"))
other_act_data       <- safe_readRDS(here::here("data", "interim", "other_act_data.rds"))
registered_products  <- safe_readRDS(here::here("data", "interim", "registered_products.rds"))
survey_data          <- safe_readRDS(here::here("data", "interim", "survey_data.rds"))

# -------------------------------------------------------------------------
# Basic Integrity Checks Before Cleaning
# -------------------------------------------------------------------------
check_integrity(opps_data,           "opps_data",           c("ID", "Customer ID", "Account", "Start Date", "Close Date"))
check_integrity(quotes_data,         "quotes_data")
check_integrity(sap_software_data,   "sap_software_data")
check_integrity(sap_machines_data,   "sap_machines_data")
check_integrity(sis_installed_base,  "sis_installed_base")
check_integrity(sis_cases,           "sis_cases")
check_integrity(sis_missions,        "sis_missions")
check_integrity(customer_data,       "customer_data")
check_integrity(contacts_data,       "contacts_data")
check_integrity(meetings_data,       "meetings_data")
check_integrity(mails_data,          "mails_data")
check_integrity(other_act_data,      "other_act_data")
check_integrity(registered_products, "registered_products")
check_integrity(survey_data,         "survey_data")

# -------------------------------------------------------------------------
# Cleaning Functions (minimal examples for each dataset)
# -------------------------------------------------------------------------

cat("Names of opps_data:\n")
print(names(opps_data))

clean_opps_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE) # convert strings to numeric/dates if possible
  df <- as.data.table(df)
  
  df_clean <- df %>%
    rename(
      TO_id              = `ID`,
      TO_customer_id     = `Customer ID`,
      TO_customer_name   = Account,
      TO_start_date      = `Start Date`,
      TO_close_date      = `Close Date`
    ) %>%
    mutate(
      TO_start_date = as.Date(TO_start_date),
      TO_close_date = as.Date(TO_close_date),
      TO_start_year  = year(TO_start_date),
      TO_start_month = month(TO_start_date),
      TO_close_year  = year(TO_close_date),
      TO_close_month = month(TO_close_date)
    ) %>%
    distinct()
  return(df_clean)
}

clean_quotes_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  # Minimal example: just remove duplicates
  df_clean <- distinct(df)
  return(df_clean)
}

clean_sap_software <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- df %>%
    rename(
      ts_year        = geschaftsj_periode, # `Geschäftsj./Periode`, ## wurde wegen janitor::clean_names() anders
      ts_customer_id = endkunde,
      ts_revenue     = umsatz
    ) %>%
    mutate(
      ts_year    = as.numeric(ts_year),
      ts_revenue = as.numeric(ts_revenue)
    ) %>%
    filter(!is.na(ts_year)) %>%
    distinct()
  return(df_clean)
}

clean_sap_machines <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  # Minimal cleaning example
  df_clean <- distinct(df)
  return(df_clean)
}

clean_sis_installed_base <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- df %>%
    mutate(
      construction_year = year(`Start-up 1 date`),
      installation_year = year(`Start-up 2 date`)
    ) %>%
    distinct()
  return(df_clean)
}

clean_sis_cases <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- df %>%
    mutate(
      case_year          = as.numeric(substr(CW, 1, 4)),
      case_calendar_week = as.numeric(substr(CW, 6, 7))
    ) %>%
    filter(`NSC status` == "Closed") %>%
    distinct()
  return(df_clean)
}

# df <- sis_missions
clean_sis_missions <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- df %>%
    mutate(
      mission_year          = as.numeric(substr(CW, 1, 4)),
      mission_calendar_week = as.numeric(substr(CW, 6, 7))
    ) %>%
    distinct()
  return(df_clean)
}

# clean_sis_mission_df <- clean_sis_missions(sis_missions)

clean_customer_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- df %>%
    rename(
      customer_account_id = `Account ID`,
      customer_sap_id     = `SAP ID`,
      customer_name       = Name
    ) %>%
    distinct()
  return(df_clean)
}

clean_contacts_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  # Minimal cleaning example
  df_clean <- distinct(df)
  return(df_clean)
}

clean_meetings_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- distinct(df)
  return(df_clean)
}

clean_mails_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- distinct(df)
  return(df_clean)
}

clean_other_act_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- distinct(df)
  return(df_clean)
}

clean_registered_products <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- distinct(df)
  return(df_clean)
}

clean_survey_data <- function(df) {
  df <- as.data.frame(df)
  df <- type.convert(df, as.is = TRUE)
  df <- as.data.table(df)
  
  df_clean <- distinct(df)
  return(df_clean)
}

# -------------------------------------------------------------------------
# Main Execution
# -------------------------------------------------------------------------
main <- function() {

  # 1) Clean each dataset if not NULL
  if (!is.null(opps_data))           opps_data           <<- clean_opps_data(opps_data)
  # if (!is.null(quotes_data))         quotes_data         <<- clean_quotes_data(quotes_data)
  if (!is.null(sap_software_data))   sap_software_data   <<- clean_sap_software(sap_software_data)
  # if (!is.null(sap_machines_data))   sap_machines_data   <<- clean_sap_machines(sap_machines_data)
  # if (!is.null(sis_installed_base))  sis_installed_base  <<- clean_sis_installed_base(sis_installed_base)
  # if (!is.null(sis_cases))           sis_cases           <<- clean_sis_cases(sis_cases)
  # if (!is.null(sis_missions))        sis_missions        <<- clean_sis_missions(sis_missions)
  # if (!is.null(customer_data))       customer_data       <<- clean_customer_data(customer_data)
  # if (!is.null(contacts_data))       contacts_data       <<- clean_contacts_data(contacts_data)
  # if (!is.null(meetings_data))       meetings_data       <<- clean_meetings_data(meetings_data)
  # if (!is.null(mails_data))          mails_data          <<- clean_mails_data(mails_data)
  # if (!is.null(other_act_data))      other_act_data      <<- clean_other_act_data(other_act_data)
  # if (!is.null(registered_products)) registered_products <<- clean_registered_products(registered_products)
  # if (!is.null(survey_data))         survey_data         <<- clean_survey_data(survey_data)

  # 2) Optional: re-check integrity after cleaning
  # check_integrity(opps_data,           "opps_data_cleaned")
  # check_integrity(quotes_data,         "quotes_data_cleaned")
  # check_integrity(sap_software_data,   "sap_software_data_cleaned")
  # check_integrity(sap_machines_data,   "sap_machines_data_cleaned")
  # check_integrity(sis_installed_base,  "sis_installed_base_cleaned")
  # check_integrity(sis_cases,           "sis_cases_cleaned")
  # check_integrity(sis_missions,        "sis_missions_cleaned")
  # check_integrity(customer_data,       "customer_data_cleaned")
  # check_integrity(contacts_data,       "contacts_data_cleaned")
  # check_integrity(meetings_data,       "meetings_data_cleaned")
  # check_integrity(mails_data,          "mails_data_cleaned")
  # check_integrity(other_act_data,      "other_act_data_cleaned")
  # check_integrity(registered_products, "registered_products_cleaned")
  # check_integrity(survey_data,         "survey_data_cleaned")

  # 3) Save all cleaned data to 'data/processed'
  processed_dir <- here::here("data", "processed")
  if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)

  if (!is.null(opps_data))           saveRDS(opps_data,           file = file.path(processed_dir, "opps_data_cleaned.rds"))
  if (!is.null(quotes_data))         saveRDS(quotes_data,         file = file.path(processed_dir, "quotes_data_cleaned.rds"))
  if (!is.null(sap_software_data))   saveRDS(sap_software_data,   file = file.path(processed_dir, "sap_software_cleaned.rds"))
  if (!is.null(sap_machines_data))   saveRDS(sap_machines_data,   file = file.path(processed_dir, "sap_machines_cleaned.rds"))
  if (!is.null(sis_installed_base))  saveRDS(sis_installed_base,  file = file.path(processed_dir, "sis_installed_base_cleaned.rds"))
  if (!is.null(sis_cases))           saveRDS(sis_cases,           file = file.path(processed_dir, "sis_cases_cleaned.rds"))
  if (!is.null(sis_missions))        saveRDS(sis_missions,        file = file.path(processed_dir, "sis_missions_cleaned.rds"))
  if (!is.null(customer_data))       saveRDS(customer_data,       file = file.path(processed_dir, "customer_data_cleaned.rds"))
  if (!is.null(contacts_data))       saveRDS(contacts_data,       file = file.path(processed_dir, "contacts_data_cleaned.rds"))
  if (!is.null(meetings_data))       saveRDS(meetings_data,       file = file.path(processed_dir, "meetings_data_cleaned.rds"))
  if (!is.null(mails_data))          saveRDS(mails_data,          file = file.path(processed_dir, "mails_data_cleaned.rds"))
  if (!is.null(other_act_data))      saveRDS(other_act_data,      file = file.path(processed_dir, "other_act_data_cleaned.rds"))
  if (!is.null(registered_products)) saveRDS(registered_products, file = file.path(processed_dir, "registered_products_cleaned.rds"))
  if (!is.null(survey_data))         saveRDS(survey_data,         file = file.path(processed_dir, "survey_data_cleaned.rds"))

  cat("All data cleaned and saved in 'data/processed'!\n")
}


## Claude version; funktioniert leider nicht.
# main <- function() {
#   # Source our debug functions
#   source(here::here("scripts", "debug_functions.R"))  # Save the first artifact as this file
#   
#   cat("\nStarting data cleaning process...\n")
#   
#   # Create a list to track all datasets and their cleaning status
#   datasets <- list(
#     list(name = "opps_data", data = opps_data, clean_fn = clean_opps_data),
#     list(name = "quotes_data", data = quotes_data, clean_fn = clean_quotes_data),
#     list(name = "sap_software_data", data = sap_software_data, clean_fn = clean_sap_software),
#     list(name = "sap_machines_data", data = sap_machines_data, clean_fn = clean_sap_machines),
#     list(name = "sis_installed_base", data = sis_installed_base, clean_fn = clean_sis_installed_base),
#     list(name = "sis_cases", data = sis_cases, clean_fn = clean_sis_cases),
#     list(name = "sis_missions", data = sis_missions, clean_fn = clean_sis_missions),
#     list(name = "customer_data", data = customer_data, clean_fn = clean_customer_data),
#     list(name = "contacts_data", data = contacts_data, clean_fn = clean_contacts_data),
#     list(name = "meetings_data", data = meetings_data, clean_fn = clean_meetings_data),
#     list(name = "mails_data", data = mails_data, clean_fn = clean_mails_data),
#     list(name = "other_act_data", data = other_act_data, clean_fn = clean_other_act_data),
#     list(name = "registered_products", data = registered_products, clean_fn = clean_registered_products),
#     list(name = "survey_data", data = survey_data, clean_fn = clean_survey_data)
#   )
#   
#   # Process each dataset
#   processed_dir <- here::here("data", "processed")
#   dir.create(processed_dir, showWarnings = FALSE, recursive = TRUE)
#   
#   for (ds in datasets) {
#     cat("\nProcessing", ds$name, "...\n")
#     
#     # Check if data exists
#     if (is.null(ds$data)) {
#       cat("⚠️ Skipping", ds$name, "- data is NULL\n")
#       next
#     }
#     
#     # Run integrity check before cleaning
#     check_integrity_debug(ds$data, paste0(ds$name, "_before_cleaning"))
#     
#     # Clean the data
#     tryCatch({
#       cleaned_data <- ds$clean_fn(ds$data)
#       cat("✓ Cleaning completed for", ds$name, "\n")
#       
#       # Check integrity after cleaning
#       check_integrity_debug(cleaned_data, paste0(ds$name, "_after_cleaning"))
#       
#       # Save the cleaned data
#       output_file <- file.path(processed_dir, paste0(ds$name, "_cleaned.rds"))
#       if (debug_save_rds(cleaned_data, output_file)) {
#         cat("✓ Successfully processed and saved", ds$name, "\n")
#       }
#     }, error = function(e) {
#       cat("❌ Error processing", ds$name, ":", e$message, "\n")
#     })
#   }
#   
#   cat("\nData cleaning process completed!\n")
# }

# -------------------------------------------------------------------------
# Run main() if script is executed directly (non-interactive mode)
# -------------------------------------------------------------------------
if (!interactive()) {
  main()
}
