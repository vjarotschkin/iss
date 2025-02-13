# 00_setup.R
# Purpose: Set up project environment and create necessary directories

# Load required packages
if(!require("pacman")) install.packages("pacman")
pacman::p_load(
  here,
  tidyverse,
  data.table,
  lubridate,
  readxl
)

# Create necessary directories if they don't exist
dir.create(here::here("data"), showWarnings = FALSE)
dir.create(here::here("data", "interim"), showWarnings = FALSE)
dir.create(here::here("data", "processed"), showWarnings = FALSE)
dir.create(here::here("data", "final"), showWarnings = FALSE)

# Set options
options(scipen = 999)