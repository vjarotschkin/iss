# 03_panel_creation_datatable.R
# Purpose: Create panel dataset using data.table for better performance

# Setup -------------------------------------------------------------------
if(!require("pacman")) install.packages("pacman")
pacman::p_load(
  here,
  data.table,
  zoo
)

# Load cleaned data ------------------------------------------------------
load(here::here("data", "processed", "opps_cleaned.RData"))
load(here::here("data", "processed", "sap_cleaned.RData"))
load(here::here("data", "processed", "sis_cleaned.RData"))
load(here::here("data", "processed", "customer_data.RData"))

# Convert to data.tables
setDT(opps_cleaned)
setDT(sap_cleaned)
setDT(sis_cleaned$installed_base)
setDT(sis_cleaned$cases)
setDT(customer_data)

# Create Base Panel Structure --------------------------------------------
create_panel_structure <- function(opps_dt, start_year = 1990, end_year = 2020) {
  # Get unique customer IDs
  customer_ids <- unique(opps_dt$TO_customer_id)
  
  # Create panel structure
  panel_dt <- CJ(
    point_in_time = start_year:end_year,
    TO_customer_id = customer_ids
  )
  
  setkey(panel_dt, TO_customer_id, point_in_time)
  return(panel_dt)
}

# Add Opportunities Data ------------------------------------------------
add_opportunities_data <- function(panel_dt, opps_dt) {
  # Aggregate opportunities data
  opps_agg <- opps_dt[, .(
    TO_sum_opps_won = sum(TO_opportunity_status == "Won", na.rm = TRUE),
    TO_sum_val_opps_won = sum(ifelse(TO_opportunity_status == "Won", TO_expected_value, 0), na.rm = TRUE)
  ), by = .(TO_customer_id, TO_closing_year)]
  
  setnames(opps_agg, "TO_closing_year", "point_in_time")
  setkey(opps_agg, TO_customer_id, point_in_time)
  
  # Join with panel
  panel_dt[opps_agg, 
           `:=`(
             TO_sum_opps_won = i.TO_sum_opps_won,
             TO_sum_val_opps_won = i.TO_sum_val_opps_won
           )
  ]
  
  return(panel_dt)
}

# Add SAP Data ---------------------------------------------------------
add_sap_data <- function(panel_dt, sap_dt) {
  # Aggregate SAP data
  sap_agg <- sap_dt[, .(
    TS_sales_total = sum(TS_revenue, na.rm = TRUE)
  ), by = .(TS_customer_id, TS_year)]
  
  setnames(sap_agg, c("TS_customer_id", "TS_year"), c("TO_customer_id", "point_in_time"))
  setkey(sap_agg, TO_customer_id, point_in_time)
  
  # Join with panel
  panel_dt[sap_agg, TS_sales_total := i.TS_sales_total]
  
  return(panel_dt)
}

# Add SiS Data ---------------------------------------------------------
add_sis_data <- function(panel_dt, sis_dt) {
  # Aggregate installed base data
  installed_base_agg <- sis_dt$installed_base[, .(
    installed_hw_power_bi = .N
  ), by = .(Customer_number = `Customer number`, construction_year)]
  
  setnames(installed_base_agg, 
           c("Customer_number", "construction_year"), 
           c("TO_customer_id", "point_in_time"))
  setkey(installed_base_agg, TO_customer_id, point_in_time)
  
  # Aggregate cases data
  cases_agg <- sis_dt$cases[, .(
    total_cases = .N
  ), by = .(Customer_number = `Customer number`, case_year)]
  
  setnames(cases_agg, 
           c("Customer_number", "case_year"), 
           c("TO_customer_id", "point_in_time"))
  setkey(cases_agg, TO_customer_id, point_in_time)
  
  # Join with panel
  panel_dt[installed_base_agg, installed_hw_power_bi := i.installed_hw_power_bi]
  panel_dt[cases_agg, total_cases := i.total_cases]
  
  return(panel_dt)
}

# Calculate Rolling Averages and Cumulative Sums ------------------------
calculate_time_metrics <- function(panel_dt) {
  # Order by customer and time for calculations
  setorder(panel_dt, TO_customer_id, point_in_time)
  
  # Calculate by group
  panel_dt[, `:=`(
    # Rolling averages (using zoo::rollmean)
    sales_ma_3 = zoo::rollmean(TO_sum_val_opps_won, k = 3, align = "right", fill = NA),
    sales_ma_6 = zoo::rollmean(TO_sum_val_opps_won, k = 6, align = "right", fill = NA),
    
    # Cumulative sums
    total_sales_cum = cumsum(replace(TO_sum_val_opps_won, is.na(TO_sum_val_opps_won), 0)),
    total_cases_cum = cumsum(replace(total_cases, is.na(total_cases), 0))
  ), by = TO_customer_id]
  
  return(panel_dt)
}

# Main Execution --------------------------------------------------------
main <- function() {
  # Create panel structure
  panel_dt <- create_panel_structure(opps_cleaned)
  
  # Add data from different sources
  panel_dt <- panel_dt %>%
    add_opportunities_data(opps_cleaned) %>%
    add_sap_data(sap_cleaned) %>%
    add_sis_data(sis_cleaned) %>%
    calculate_time_metrics()
  
  # Save final panel dataset
  save(panel_dt, file = here::here("data", "final", "panel_dataset.RData"))
}

# Run main function if script is run directly
if (!interactive()) {
  main()
}