# 00_setup.R
# Purpose: Set up project environment and create necessary directories

# Load required packages
# if(!require("pacman")) install.packages("pacman")
if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman")
}
# Setup -------------------------------------------------------------------
# if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  janitor,
  here,
  ggplot2,
  tidyverse,
  lubridate,
  data.table,
  dplyr,
  stringr,
  here,
  data.table,
  tidyverse,
  lubridate,
  readxl,
  janitor
)

# Create necessary directories if they don't exist
dir.create(here::here("data"), showWarnings = FALSE)
dir.create(here::here("data", "interim"), showWarnings = FALSE)
dir.create(here::here("data", "external"), showWarnings = FALSE)
dir.create(here::here("data", "processed"), showWarnings = FALSE)
dir.create(here::here("data", "final"), showWarnings = FALSE)


# Create necessary directories (if not already present)
dir.create(here::here("data", "interim"), showWarnings = FALSE, recursive = TRUE)

# Set options
options(scipen = 999)

## Viktor: Saved the data loading script here instead to have a better pipeline flow
###############################################################################
# 01_data_loading.Rx``
# Purpose: Load all raw data files from various sources and save as 
#          intermediate .rds files.
#
# Project structure (directories relative to the project root):
#   - data/raw: raw data files
#   - data/interim: intermediate RDS files
#   - data/processed: cleaned data files
#   - data/final: final outputs
###############################################################################

# Setup -----------------------------------------------------------------------
# if(!require("pacman")) install.packages("pacman")
# pacman::p_load(
#   here,
#   data.table,
#   tidyverse,
#   lubridate,
#   readxl,
#   janitor
# )

# Create necessary directories (if not already present)
dir.create(here::here("data", "interim"), showWarnings = FALSE, recursive = TRUE)

###############################################################################
# Helper Functions
###############################################################################

# Safely read an Excel file with error handling.
safe_read_excel <- function(file_path, sheet = 1, skip = 0, col_types = NULL, ...) {
  if (!file.exists(file_path)) {
    warning("File not found: ", file_path)
    return(NULL)
  }
  tryCatch({
    if (!is.null(col_types)) {
      readxl::read_excel(file_path, sheet = sheet, skip = skip, col_types = col_types, ...)
    } else {
      readxl::read_excel(file_path, sheet = sheet, skip = skip, guess_max = 10000, ...)
    }
  }, error = function(e) {
    warning("Error reading file ", file_path, ": ", e$message)
    return(NULL)
  })
}


# Read and combine multiple Excel files into one data.table.
read_and_bind <- function(file_paths, sheet = 1, skip = 0, col_types = NULL) {
  dfs <- lapply(file_paths, function(fp) {
    message("Reading file: ", basename(fp))
    df <- safe_read_excel(fp, sheet = sheet, skip = skip, col_types = col_types)
    if (!is.null(df)) as.data.table(df) else NULL
  })
  dfs <- dfs[!sapply(dfs, is.null)]
  if (length(dfs) == 0) {
    warning("No files were loaded successfully.")
    return(NULL)
  }
  rbindlist(dfs, fill = TRUE)
}

###############################################################################
# Data Loading Functions
###############################################################################

# 1. CRM Opportunities --------------------------------------------------------
load_opportunities <- function() {
  message("Loading CRM Opportunities data...")
  opps_path <- here::here("data", "raw", "crm", "T_Opportunitys")
  opps_files <- list.files(path = opps_path, pattern = "del.*\\.xlsx", 
                           recursive = TRUE, full.names = TRUE)
  if(length(opps_files) == 0) {
    warning("No opportunities files found in ", opps_path)
    return(NULL)
  }
  message("Found ", length(opps_files), " opportunity files")
  # Load all files as text to avoid type coercion warnings
  opps_data <- read_and_bind(opps_files
                             , sheet = 1, skip = 4
                             # , col_types = "text"
                             
  )
  return(opps_data)
}

# 2. CRM Quotes ----------------------------------------------------------------
load_quotes <- function() {
  message("Loading CRM Quotes data...")
  quotes_path <- here::here("data", "raw", "crm", "T_Quotes")
  quotes_files <- list.files(path = quotes_path, pattern = "all_quotes.*\\.xlsx", 
                             full.names = TRUE)
  if(length(quotes_files) == 0) {
    warning("No quotes files found in ", quotes_path)
    return(NULL)
  }
  message("Found ", length(quotes_files), " quotes files")
  quotes_data <- read_and_bind(quotes_files, sheet = 1, skip = 4
                               # , col_types = "text"
                               
  )
  return(quotes_data)
}

# 3. SAP Software --------------------------------------------------------------
load_sap_software <- function() {
  message("Loading SAP Software data...")
  sap_file <- here::here("data", "raw", "sap", "Software_nach PHN_Material_nach_2020.xlsx")
  if(!file.exists(sap_file)) {
    warning("SAP Software file not found: ", sap_file)
    return(NULL)
  }
  sap_data <- safe_read_excel(sap_file, sheet = 4
                              # col_types = "text"
                              
  )
  if(!is.null(sap_data)) {
    sap_data <- janitor::clean_names(sap_data)
    sap_data <- as.data.table(sap_data)
    message("Successfully loaded SAP Software data")
  }
  return(sap_data)
}

# 4. SAP Machines --------------------------------------------------------------
load_sap_machines <- function() {
  message("Loading SAP Machines data...")
  machines_file <- here::here("data", "raw", "sap", "EXPORT_Lieferplan_14_20_changed_date_fail_vaj.xlsx")
  if(!file.exists(machines_file)) {
    warning("SAP Machines file not found: ", machines_file)
    return(NULL)
  }
  machines <- safe_read_excel(machines_file) %>% janitor::clean_names()
  # Join with additional SAP files
  vbak_file <- here::here("data", "raw", "sap", "EXPORT_VBAK.xlsx")
  vbpa_file <- here::here("data", "raw", "sap", "EXPORT_VBPA.xlsx")
  
  if(file.exists(vbak_file)) {
    vbak <- safe_read_excel(vbak_file
                            # , col_types = "text"
                            
    ) %>% janitor::clean_names()
    machines <- left_join(machines, vbak, by = c("nummer_vertriebsauftrag" = "verkaufsbeleg"))
  } else {
    warning("SAP VBAK file not found: ", vbak_file)
  }
  
  if(file.exists(vbpa_file)) {
    vbpa <- safe_read_excel(vbpa_file
                            # , col_types = "text"
                            
    ) %>% janitor::clean_names()
    machines <- left_join(machines, vbpa, by = c("nummer_vertriebsauftrag" = "vertriebsbeleg"))
  } else {
    warning("SAP VBPA file not found: ", vbpa_file)
  }
  
  machines <- as.data.table(machines)
  message("Loaded SAP Machines data with ", nrow(machines), " records")
  return(machines)
}

# 5. SiS Data (Installed Base, Cases, Missions) ------------------------------
load_sis_data <- function() {
  message("Loading SiS data...")
  sis_base_path <- here::here("data", "raw", "service-system", "SiS_Power_BI", "Installed_base_Power_BI")
  base_files <- list.files(path = sis_base_path
                           , pattern = "data.*\\.xlsx", full.names = TRUE)
  
  if(length(base_files) == 0) {
    warning("No SiS Installed Base files found in ", sis_base_path)
    installed_base <- NULL
  } else {
    message("Found ", length(base_files), " Installed Base files")
    installed_base <- read_and_bind(base_files, sheet = 1, skip = 2)
  }
  
  sis_cases_path <- here::here("data", "raw"
                               , "service-system"
                               , "SiS_Power_BI"
                               , "Cases_Power_BI")
  cases_files <- list.files(path = sis_cases_path
                            , pattern = "data.*\\.xlsx", full.names = TRUE)
  
  if(length(cases_files) == 0) {
    warning("No SiS Cases files found in ", sis_cases_path)
    cases <- NULL
  } else {
    message("Found ", length(cases_files), " Cases files")
    cases <- read_and_bind(cases_files, sheet = 1, skip = 2)
  }
  
  sis_missions_path <- here::here("data", "raw", "service-system", "SiS_Power_BI", "Missions_Power_BI")
  missions_files <- list.files(path = sis_missions_path, pattern = "data.*\\.xlsx", full.names = TRUE)
  
  if(length(missions_files) == 0) {
    warning("No SiS Missions files found in ", sis_missions_path)
    missions <- NULL
  } else {
    message("Found ", length(missions_files), " Missions files")
    missions <- read_and_bind(missions_files, sheet = 1, skip = 2)
    ## machen den foldenden Command lieber im Data Cleaning Script
    # if(!is.null(missions)) {
    #   missions <- missions %>% as.data.table() %>%
    #     .[, `:=`(
    #       mission_year = as.numeric(substr(CW, 1, 4)),
    #       mission_calendar_week = as.numeric(substr(CW, 6, 7))
    #     )] %>% .[, CW := NULL]
    # }
  }
  
  return(list(installed_base = installed_base, cases = cases, missions = missions))
}

# 6. Customer Data ------------------------------------------------------------
load_customer_data <- function() {
  message("Loading Customer data...")
  cust_path <- here::here("data", "raw", "c4c", "t-customers")
  cust_files <- list.files(path = cust_path, pattern = "Kundenliste.*\\.xlsx", full.names = TRUE)
  if(length(cust_files) == 0) {
    warning("No customer files found in ", cust_path)
    return(NULL)
  }
  message("Found ", length(cust_files), " customer files")
  cust_data <- read_and_bind(cust_files, sheet = 1, skip = 4)
  if(!is.null(cust_data)) {
    cust_data <- as.data.table(cust_data)[, .(`Account ID`, `SAP ID`, Name)]
  }
  return(cust_data)
}

# 7. Contacts Data ------------------------------------------------------------
load_contacts_data <- function() {
  message("Loading Contacts data...")
  contacts_path <- here::here("data", "raw", "crm", "T_DATA", "T_Ansprechpartner")
  contacts_files <- list.files(path = contacts_path, pattern = "Ansprechpartner.*\\.xlsx", full.names = TRUE)
  if(length(contacts_files) == 0) {
    warning("No contacts files found in ", contacts_path)
    return(NULL)
  }
  contacts_data <- read_and_bind(contacts_files, sheet = 1, skip = 4)
  if(!is.null(contacts_data)) {
    contacts_data <- as.data.table(contacts_data)[ , !c("Phone", "Mobile", "E-Mail"), with = FALSE]
  }
  return(contacts_data)
}

# 8. Meetings Data ------------------------------------------------------------
load_meetings_data <- function() {
  message("Loading Meetings data...")
  meetings_path <- here::here("data", "raw", "c4c", "t-meetings")
  meetings_files <- list.files(path = meetings_path, pattern = "ListederAktivitaeten.*\\.xlsx", full.names = TRUE)
  if(length(meetings_files) == 0) {
    warning("No meetings files found in ", meetings_path)
    return(NULL)
  }
  meetings_data <- read_and_bind(meetings_files, sheet = 1, skip = 4)
  return(meetings_data)
}

# 9. Mails Data ---------------------------------------------------------------
load_mails_data <- function() {
  message("Loading Mails data...")
  mails_path <- here::here("data", "raw", "c4c", "t-mails")
  mails_files <- list.files(path = mails_path, pattern = "Liste.*\\.xlsx", full.names = TRUE)
  if(length(mails_files) == 0) {
    warning("No mails files found in ", mails_path)
    return(NULL)
  }
  mails_data <- read_and_bind(mails_files, sheet = 1, skip = 4)
  return(mails_data)
}

# 10. Other Activities Data ---------------------------------------------------
load_other_act_data <- function() {
  message("Loading Other Activities data...")
  other_path <- here::here("data", "raw", "c4c", "t-other-activities")
  other_files <- list.files(path = other_path, pattern = "Liste.*\\.xlsx", full.names = TRUE)
  if(length(other_files) == 0) {
    warning("No other activities files found in ", other_path)
    return(NULL)
  }
  other_data <- read_and_bind(other_files, sheet = 1, skip = 4)
  return(other_data)
}

# 11. Registered Products -----------------------------------------------------
load_registered_products <- function() {
  message("Loading Registered Products data...")
  products_path <- here::here("data", "raw", "c4c", "t-products")
  prod_files <- list.files(path = products_path, pattern = "Liste.*\\.xlsx", full.names = TRUE)
  if(length(prod_files) == 0) {
    warning("No registered products files found in ", products_path)
    return(NULL)
  }
  # For each file, determine column types based on header names
  prod_list <- lapply(prod_files, function(file) {
    nms <- names(safe_read_excel(file, skip = 4, n_max = 0))
    ct <- ifelse(grepl("^Shipping", nms), "date", "guess")
    message("Reading registered products file: ", basename(file))
    dt <- safe_read_excel(file, sheet = 1, skip = 4, col_types = ct)
    if(!is.null(dt)) as.data.table(dt) else NULL
  })
  prod_list <- prod_list[!sapply(prod_list, is.null)]
  if (length(prod_list) == 0) return(NULL)
  prod_data <- rbindlist(prod_list, fill = TRUE)
  # Optionally rename columns to include a suffix for clarity
  setnames(prod_data, old = names(prod_data), new = paste0(names(prod_data), "_reg_prod"))
  return(prod_data)
}

# 12. Survey Exploratory Moderators -------------------------------------------
load_exploration_moderators <- function() {
  message("Loading Survey Exploratory Moderators data...")
  survey_file <- here::here("data", "raw", "survey", "Copy of Copy of 210517_sw_at_trumpf_extended_vaj.xlsx")
  if(!file.exists(survey_file)) {
    warning("Survey file not found: ", survey_file)
    return(NULL)
  }
  survey_data <- safe_read_excel(survey_file, sheet = "Survey_EN", skip = 1)
  if(!is.null(survey_data)) survey_data <- as.data.table(survey_data)
  return(survey_data)
}

###############################################################################
# Main Execution
###############################################################################
main <- function() {
  message("Starting data loading process...")
  
  opps_data         <- load_opportunities()
  quotes_data       <- load_quotes()
  sap_software_data <- load_sap_software()
  sap_machines_data <- load_sap_machines()
  sis_data          <- load_sis_data()       # List: installed_base, cases, missions
  customer_data     <- load_customer_data()
  contacts_data     <- load_contacts_data()
  meetings_data     <- load_meetings_data()
  mails_data        <- load_mails_data()
  other_act_data    <- load_other_act_data()
  prod_data         <- load_registered_products()
  survey_data       <- load_exploration_moderators()
  
  interim_dir <- here::here("data", "interim")
  
  message("Saving loaded data as .rds files...")
  if(!is.null(opps_data))         saveRDS(opps_data,         file = file.path(interim_dir, "opps_data.rds"))
  if(!is.null(quotes_data))       saveRDS(quotes_data,       file = file.path(interim_dir, "quotes_data.rds"))
  if(!is.null(sap_software_data)) saveRDS(sap_software_data, file = file.path(interim_dir, "sap_software.rds"))
  if(!is.null(sap_machines_data)) saveRDS(sap_machines_data, file = file.path(interim_dir, "sap_machines.rds"))
  if(!is.null(sis_data$installed_base)) saveRDS(sis_data$installed_base, file = file.path(interim_dir, "sis_installed_base.rds"))
  if(!is.null(sis_data$cases))          saveRDS(sis_data$cases,          file = file.path(interim_dir, "sis_cases.rds"))
  if(!is.null(sis_data$missions))       saveRDS(sis_data$missions,       file = file.path(interim_dir, "sis_missions.rds"))
  if(!is.null(customer_data))     saveRDS(customer_data,     file = file.path(interim_dir, "customer_data.rds"))
  if(!is.null(contacts_data))     saveRDS(contacts_data,     file = file.path(interim_dir, "contacts_data.rds"))
  if(!is.null(meetings_data))     saveRDS(meetings_data,     file = file.path(interim_dir, "meetings_data.rds"))
  if(!is.null(mails_data))        saveRDS(mails_data,        file = file.path(interim_dir, "mails_data.rds"))
  if(!is.null(other_act_data))    saveRDS(other_act_data,    file = file.path(interim_dir, "other_act_data.rds"))
  if(!is.null(prod_data))         saveRDS(prod_data,         file = file.path(interim_dir, "registered_products.rds"))
  if(!is.null(survey_data))       saveRDS(survey_data,       file = file.path(interim_dir, "survey_data.rds"))
  
  message("Data loading process completed.")
}

# Run main when script is executed
if (sys.nframe() == 0) {
  main()
}