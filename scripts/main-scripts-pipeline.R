# main.R

# 1. Initial setup
source(here::here("scripts", "00_setup.R")) # Load required packages and create directories if necessary

# 2. Source all required functions
source(here::here("scripts", "01_data_loading.R"))
main()
# Execute immediately
# opps_data <- load_opportunities()
# ## Save opps data with here package as opps_data.RData
# save(opps_data, file = here("data", "interim", "opps_data.RData"))
# 
# sap_data <- load_sap_data()
# ## Save sap data with here package as sap_data.RData
# save(sap_data, file = here("data", "interim", "sap_data.RData"))
# 
# sis_data <- load_sis_data()
# ## Save sis data with here package as sis_data.RData
# save(sis_data, file = here("data", "interim", "sis_data.RData"))
# 
# customer_data <- load_customer_data()
# ## Save customer data with here package as customer_data.RData
# save(customer_data, file = here("data", "interim", "customer_data.RData"))


source(here::here("scripts", "02_data_cleaning.R"))
main()

create_complete_panel()
source(here::here("scripts", "03_panel_creation.R"))
# source(here::here("scripts", "04_feature_eng.R"))
# source(here::here("R", "utils", "validation.R"))
# source(here::here("R", "utils", "helpers.R"))

# 3. Execute pipeline
message("Starting data pipeline...")

# Load data
message("Loading data...")
crm_dt <- load_crm_data(here("data", "raw", "CRM"))
service_dt <- load_service_data(here("data", "raw", "Service"))
transaction_dt <- load_transaction_data(here("data", "raw", "Transactions"))

# Save interim results
saveRDS(crm_dt, here("data", "interim", "crm_raw.rds"))
saveRDS(service_dt, here("data", "interim", "service_raw.rds"))
saveRDS(transaction_dt, here("data", "interim", "transaction_raw.rds"))

# Clean data
message("Cleaning data...")
crm_dt <- clean_crm_data(crm_dt)
service_dt <- clean_service_data(service_dt)
transaction_dt <- clean_transaction_data(transaction_dt)

# Create and merge panel
message("Creating panel structure...")
panel_dt <- create_panel(crm_dt)
panel_dt <- merge_panel_data(panel_dt, crm_dt)
panel_dt <- merge_panel_data(panel_dt, service_dt)
panel_dt <- merge_panel_data(panel_dt, transaction_dt)

# Engineer features
message("Engineering features...")
panel_dt <- engineer_features(panel_dt)

# Save final dataset
message("Saving processed data...")
saveRDS(panel_dt, here("data", "processed", "final_panel.rds"))

message("Pipeline completed successfully!")