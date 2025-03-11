# 02_improved_data_cleaning.R
# Purpose: Validate, clean, and transform loaded data for exploratory analysis

# Load required packages
if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman")
}

pacman::p_load(
  janitor,
  here,
  ggplot2,
  tidyverse,
  lubridate,
  data.table,
  dplyr,
  stringr,
  readr,  # Added for improved parsing
  zoo  # For rolling functions
)

# Set options
options(scipen = 999)
options(warn = 1)  # Print warnings as they occur

# A safe way to read RDS files (warn if missing)
safe_readRDS <- function(path) {
  if (file.exists(path)) {
    readRDS(path)
  } else {
    warning("File not found: ", path)
    return(NULL)
  }
}

# Improved type conversion function with detailed logging and function comparison fix
convert_types <- function(df, numeric_cols = NULL, date_cols = NULL, 
                          logical_cols = NULL, factor_cols = NULL,
                          df_name = "unknown") {
  
  # Output for diagnostic logging
  log_path <- here::here("logs")
  dir.create(log_path, showWarnings = FALSE, recursive = TRUE)
  log_file <- file.path(log_path, paste0("type_conversion_", df_name, ".log"))
  
  # Start a log file for this dataset
  cat(paste0("Type conversion log for ", df_name, " - ", Sys.time(), "\n\n"), 
      file = log_file, append = FALSE)
  
  # Add special handling for timestamp columns
  timestamp_cols <- intersect(c("changed_on", "created_on", "start_date_time", "end_date_time", 
                                "last_changed_on", "sent_on"), date_cols)
  if (length(timestamp_cols) > 0) {
    for (col in timestamp_cols) {
      if (col %in% names(df) && !inherits(df[[col]], "POSIXct") && !inherits(df[[col]], "Date")) {
        cat(paste0("Special handling for timestamp column: ", col, "\n"), file = log_file, append = TRUE)
        
        # Make sure we're working with character data
        if (!is.character(df[[col]])) {
          df[[col]] <- as.character(df[[col]])
        }
        
        # Get sample values for diagnosis
        sample_vals <- head(na.omit(df[[col]]), 5)
        cat(paste0("  Sample values: ", paste(sample_vals, collapse = ", "), "\n"), 
            file = log_file, append = TRUE)
        
        # Try direct conversion first using as.POSIXct
        formats <- c("%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
                     "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S")
        
        converted <- FALSE
        for (fmt in formats) {
          if (converted) break
          
          tryCatch({
            result <- suppressWarnings(as.POSIXct(df[[col]], format = fmt))
            if (sum(!is.na(result)) > 0) {
              df[[col]] <- result
              cat(paste0("  Used as.POSIXct with format: ", fmt, "\n"), file = log_file, append = TRUE)
              converted <- TRUE
              break
            }
          }, error = function(e) {
            # Just skip to next format
          })
        }
        
        # If direct conversion failed, try lubridate
        if (!converted) {
          tryCatch({
            # Try to parse with lubridate
            formats <- c("ymd HMS", "dmy HMS", "mdy HMS", "ymd HM", "dmy HM", "mdy HM")
            
            for (fmt in formats) {
              if (converted) break
              
              result <- suppressWarnings(lubridate::parse_date_time(df[[col]], orders = fmt, quiet = TRUE))
              if (sum(!is.na(result)) > 0) {
                df[[col]] <- as.POSIXct(result)
                success_rate <- sum(!is.na(df[[col]])) / length(df[[col]]) * 100
                cat(paste0("  Used lubridate with format ", fmt, ": ", round(success_rate, 2), "% success\n"), 
                    file = log_file, append = TRUE)
                converted <- TRUE
                break
              }
            }
          }, error = function(e) {
            cat(paste0("  Error with lubridate: ", e$message, "\n"), 
                file = log_file, append = TRUE)
          })
        }
        
        if (!converted) {
          cat("  Failed to convert timestamp column\n", file = log_file, append = TRUE)
        }
      }
    }
  }
  
  # Enhanced Date conversion - much more aggressive 
  if (!is.null(date_cols) && length(date_cols) > 0) {
    date_cols <- intersect(date_cols, names(df))
    if (length(date_cols) > 0) {
      cat(paste0("Converting to dates: ", paste(date_cols, collapse = ", "), "\n"), 
          file = log_file, append = TRUE)
      
      for (col in date_cols) {
        cat(paste0("Processing date column: ", col, "\n"), file = log_file, append = TRUE)
        
        # Skip if already properly converted
        if (inherits(df[[col]], "Date") || inherits(df[[col]], "POSIXct")) {
          cat("  Column is already a Date/POSIXct type, skipping\n", file = log_file, append = TRUE)
          next
        }
        
        # Get sample values for diagnosis
        sample_vals <- head(na.omit(df[[col]]), 5)
        cat(paste0("  Sample values: ", paste(sample_vals, collapse = ", "), "\n"), 
            file = log_file, append = TRUE)
        
        # Check if this looks like Excel numeric dates
        # Excel dates are typically large numbers like 33683, 43682, etc.
        if (!is.character(df[[col]])) {
          # Try to determine if these are numeric Excel dates
          if (is.numeric(df[[col]])) {
            # Check if values are in a reasonable Excel date range
            min_val <- min(df[[col]], na.rm = TRUE)
            max_val <- max(df[[col]], na.rm = TRUE)
            
            if (min_val > 10000 && max_val < 50000) {
              cat("  Column appears to contain Excel numeric dates\n", file = log_file, append = TRUE)
              date_result <- as.Date(df[[col]], origin = "1899-12-30")
              
              # Verify the conversion looks reasonable
              if (min(date_result, na.rm = TRUE) > as.Date("1900-01-01") && 
                  max(date_result, na.rm = TRUE) < as.Date("2050-12-31")) {
                df[[col]] <- date_result
                cat("  Successfully converted Excel numeric dates\n", file = log_file, append = TRUE)
                next
              }
            }
          }
          
          # If not Excel dates or conversion failed, convert to character
          df[[col]] <- as.character(df[[col]])
        }
        
        # Now try to clean the character data
        # Fix the regular expression pattern - place the hyphen at the end
        clean_values <- gsub("[^0-9./: -]", "", df[[col]])
        # Standardize separators (carefully, without using problematic regex)
        clean_values <- gsub("\\.", "-", clean_values)
        clean_values <- gsub("/", "-", clean_values)
        # Remove extra spaces
        clean_values <- trimws(gsub("\\s+", " ", clean_values))
        
        # Update the column with cleaned values
        df[[col]] <- clean_values
        
        # Log the cleaning process
        cat("  Cleaned values for better date parsing\n", file = log_file, append = TRUE)
        
        # Try a wider range of date formats with lubridate
        # Include many common date formats used in Excel, databases, etc.
        formats <- c(
          # Standard formats
          "Ymd", "dmY", "mdY", "Ydm", 
          # With separators
          "Y-m-d", "d-m-Y", "m-d-Y", "Y/m/d", "d/m/Y", "m/d/Y",
          # With timestamps
          "Ymd HMS", "dmY HMS", "mdY HMS", "Y-m-d H:M:S", "d-m-Y H:M:S", "m-d-Y H:M:S",
          # Special formats
          "a, d b Y", "a, d b Y H:M:S", "d b Y", "b d Y", "d b Y H:M:S",
          # Excel format string
          "m/d/y h:m:s", "d/m/y h:m:s"
        )
        
        # First try each format specifically
        converted <- FALSE
        for (format in formats) {
          if (converted) break
          
          tryCatch({
            result <- suppressWarnings(lubridate::parse_date_time(df[[col]], orders = format, quiet = TRUE))
            date_result <- as.Date(result)
            
            # Check how many valid conversions we got
            valid_dates <- !is.na(date_result) & 
              date_result > as.Date("1900-01-01") & 
              date_result < Sys.Date() + 365*20  # Allow future dates up to 20 years
            
            success_rate <- sum(valid_dates, na.rm = TRUE) / length(df[[col]]) * 100
            
            if (success_rate > 10) {  # Lower threshold for accepting format match
              cat(paste0("  Format ", format, " conversion rate: ", round(success_rate, 2), "%\n"), 
                  file = log_file, append = TRUE)
              df[[col]] <- date_result
              converted <- TRUE
              break  # Success! Break out of the loop
            }
          }, error = function(e) {
            # Just skip to next format on error
            cat(paste0("  Error with format ", format, ": ", e$message, "\n"), 
                file = log_file, append = TRUE)
          })
        }
        
        # If no specific format worked, try multiple formats together
        if (!converted) {
          tryCatch({
            # Try all formats at once with very permissive settings
            result <- suppressWarnings(lubridate::parse_date_time(
              df[[col]], 
              orders = formats,
              quiet = TRUE,
              exact = FALSE,
              truncated = 3  # Allow for truncated formats
            ))
            date_result <- as.Date(result)
            
            # Check conversion success
            success_rate <- sum(!is.na(date_result)) / length(df[[col]]) * 100
            cat(paste0("  Multi-format attempt: ", round(success_rate, 2), "% success\n"), 
                file = log_file, append = TRUE)
            
            if (success_rate > 0) {  # Accept any improvement
              df[[col]] <- date_result
              converted <- TRUE
            }
          }, error = function(e) {
            cat(paste0("  Multi-format attempt failed: ", e$message, "\n"), 
                file = log_file, append = TRUE)
          })
        }
        
        # Last resort: Try base R methods
        if (!converted) {
          # Try base R as.Date with various formats
          base_formats <- c("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y")
          for (fmt in base_formats) {
            if (converted) break
            
            tryCatch({
              result <- suppressWarnings(as.Date(df[[col]], format = fmt))
              if (sum(!is.na(result)) > 0) {
                df[[col]] <- result
                cat(paste0("  Used base R as.Date with format: ", fmt, "\n"), file = log_file, append = TRUE)
                converted <- TRUE
                break
              }
            }, error = function(e) {
              # Just skip to next format
            })
          }
          
          if (!converted) {
            # One last attempt - direct conversion from numeric Excel dates
            tryCatch({
              # Try to convert to numeric first
              numeric_vals <- suppressWarnings(as.numeric(df[[col]]))
              if (!all(is.na(numeric_vals))) {
                excel_result <- as.Date(numeric_vals, origin = "1899-12-30")
                valid_dates <- !is.na(excel_result) & 
                  excel_result > as.Date("1900-01-01") & 
                  excel_result < as.Date("2050-12-31")
                
                if (sum(valid_dates, na.rm = TRUE) > 0) {
                  df[[col]] <- excel_result
                  cat("  Last resort Excel date conversion successful\n", file = log_file, append = TRUE)
                  converted <- TRUE
                }
              }
            }, error = function(e) {
              # Final failure
              cat("  All date parsing methods failed\n", file = log_file, append = TRUE)
            })
          }
        }
      }
    }
  }
  
  # Helper to safely convert other column types
  safe_convert <- function(df, cols, conversion_type, type_name) {
    if (is.null(cols) || length(cols) == 0) return(df)
    
    # Filter to only include columns that actually exist in the dataframe
    cols <- intersect(cols, names(df))
    if (length(cols) == 0) return(df)
    
    # Log the columns we're converting
    cat(paste0("Converting to ", type_name, ": ", paste(cols, collapse = ", "), "\n"), 
        file = log_file, append = TRUE)
    
    # Enhanced conversion functions with more robust handling
    conversion_fn <- switch(conversion_type,
                            "numeric" = function(x) {
                              # Print sample values for diagnosis
                              sample_vals <- head(na.omit(x), 5)
                              cat(paste0("  Sample values: ", paste(sample_vals, collapse = ", "), "\n"), 
                                  file = log_file, append = TRUE)
                              
                              # First clean the strings - handle various formats
                              if (is.character(x)) {
                                # Remove percentage signs and save if they existed
                                had_percent <- grepl("%", x)
                                x <- gsub("%", "", x)
                                
                                # Remove currency symbols
                                x <- gsub("[$€£¥]", "", x)
                                
                                # Handle thousands separators and decimal points
                                # First standardize decimal points (assume period is decimal)
                                x <- gsub(",([0-9]{1,2})$", ".$1", x)  # For European format
                                
                                # Then remove thousand separators
                                x <- gsub(",", "", x)
                                
                                # Remove all whitespace
                                x <- gsub("\\s+", "", x)
                                
                                # Remove any remaining non-numeric chars
                                x <- gsub("[^0-9\\.-]", "", x)
                                
                                # Handle empty strings
                                x[x == ""] <- NA
                                
                                # Convert to numeric
                                result <- suppressWarnings(as.numeric(x))
                                
                                # Adjust values that were percentages
                                if (any(had_percent, na.rm = TRUE)) {
                                  result[had_percent] <- result[had_percent] / 100
                                }
                                
                                return(result)
                              } else {
                                return(suppressWarnings(as.numeric(x)))
                              }
                            },
                            "logical" = function(x) {
                              # Print sample values for diagnosis
                              sample_vals <- head(na.omit(x), 5)
                              cat(paste0("  Sample values: ", paste(sample_vals, collapse = ", "), "\n"), 
                                  file = log_file, append = TRUE)
                              
                              if (is.character(x)) {
                                # Handle various text representations of boolean values
                                x_lower <- tolower(trimws(x))
                                
                                # Map common string representations to TRUE/FALSE
                                result <- rep(NA, length(x))
                                result[x_lower %in% c("true", "t", "yes", "y", "1", "x")] <- TRUE
                                result[x_lower %in% c("false", "f", "no", "n", "0", "")] <- FALSE
                                
                                # If the column is mostly 0/1, treat it as logical
                                if (all(x %in% c("0", "1", NA), na.rm = TRUE)) {
                                  result[x == "1"] <- TRUE
                                  result[x == "0"] <- FALSE
                                }
                                
                                # Handle checkboxes
                                result[x_lower %in% c("checked", "selected", "on")] <- TRUE
                                result[x_lower %in% c("unchecked", "unselected", "off")] <- FALSE
                                
                                success_rate <- sum(!is.na(result)) / sum(!is.na(x)) * 100
                                cat(paste0("  Logical conversion rate: ", round(success_rate, 2), "%\n"), 
                                    file = log_file, append = TRUE)
                                
                                return(as.logical(result))
                              } else {
                                return(as.logical(x))
                              }
                            },
                            "factor" = function(x) {
                              # Print sample values for diagnosis
                              sample_vals <- head(na.omit(x), 5)
                              cat(paste0("  Sample values: ", paste(sample_vals, collapse = ", "), "\n"), 
                                  file = log_file, append = TRUE)
                              
                              # First clean the text if it's character
                              if (is.character(x)) {
                                x <- trimws(x)
                                x[x == ""] <- NA  # Empty strings to NA
                              }
                              
                              return(as.factor(x))
                            },
                            as.character)  # Default fallback
    
    for (col in cols) {
      cat(paste0("Processing column: ", col, "\n"), file = log_file, append = TRUE)
      
      # Skip if the column is already the correct type
      if (conversion_type == "numeric" && is.numeric(df[[col]])) {
        cat(paste0("  Column ", col, " is already numeric, skipping\n"), 
            file = log_file, append = TRUE)
        next
      }
      if (conversion_type == "logical" && is.logical(df[[col]])) {
        cat(paste0("  Column ", col, " is already logical, skipping\n"), 
            file = log_file, append = TRUE)
        next
      }
      if (conversion_type == "factor" && is.factor(df[[col]])) {
        cat(paste0("  Column ", col, " is already a factor, skipping\n"), 
            file = log_file, append = TRUE)
        next
      }
      
      # Special handling for known problematic column types
      if (conversion_type == "logical") {
        # Try to identify Y/N columns
        if (is.character(df[[col]]) && all(df[[col]] %in% c("Y", "N", NA), na.rm = TRUE)) {
          cat(paste0("  Detected Y/N column, using special conversion\n"), 
              file = log_file, append = TRUE)
          df[[col]] <- df[[col]] == "Y"
          next
        }
        
        # Try to identify TRUE/FALSE columns
        if (is.character(df[[col]]) && 
            all(toupper(df[[col]]) %in% c("TRUE", "FALSE", NA), na.rm = TRUE)) {
          cat(paste0("  Detected TRUE/FALSE column, using standard conversion\n"), 
              file = log_file, append = TRUE)
          df[[col]] <- toupper(df[[col]]) == "TRUE"
          next
        }
      }
      
      # Try to convert with detailed error reporting
      tryCatch({
        original_values <- df[[col]]
        
        # Apply the conversion
        result <- suppressWarnings(conversion_fn(df[[col]]))
        
        # Check conversion success rate
        valid_count <- sum(!is.na(result[!is.na(original_values)]))
        total_count <- sum(!is.na(original_values))
        
        if (total_count > 0) {
          success_rate <- valid_count / total_count * 100
          cat(paste0("  Conversion success rate: ", round(success_rate, 2), "% (", 
                     valid_count, "/", total_count, ")\n"), 
              file = log_file, append = TRUE)
          
          # Only update if we had a reasonable success rate
          if (success_rate > 5 || valid_count > 100) {  # Accept small percentages for large datasets
            df[[col]] <- result
          } else {
            cat(paste0("  Success rate too low, keeping original values\n"), 
                file = log_file, append = TRUE)
          }
        } else {
          cat(paste0("  No non-NA values to convert\n"), 
              file = log_file, append = TRUE)
        }
        
        # For numeric conversion, check for NAs introduced by coercion
        if (conversion_type == "numeric") {
          na_introduced <- sum(is.na(df[[col]]) & !is.na(original_values))
          if (na_introduced > 0) {
            problem_examples <- head(original_values[is.na(df[[col]]) & !is.na(original_values)], 5)
            cat(paste0("  NAs introduced: ", na_introduced, " (examples: ", 
                       paste(problem_examples, collapse = ", "), ")\n"), 
                file = log_file, append = TRUE)
          }
        }
      }, error = function(e) {
        cat(paste0("  Error converting ", col, " to ", type_name, ": ", e$message, "\n"), 
            file = log_file, append = TRUE)
      })
    }
    return(df)
  }
  
  # Apply conversions in sequence - note that date columns were already handled above
  df <- safe_convert(df, numeric_cols, "numeric", "numeric")
  df <- safe_convert(df, logical_cols, "logical", "logical")
  df <- safe_convert(df, factor_cols, "factor", "factor")
  
  # Check which numeric columns weren't successfully converted
  intended_numeric_cols <- numeric_cols
  actual_numeric_cols <- names(df)[sapply(df, is.numeric)]
  missed_numeric_cols <- setdiff(intended_numeric_cols, actual_numeric_cols)
  
  if (length(missed_numeric_cols) > 0) {
    cat("\nNumeric columns that failed conversion:\n", file = log_file, append = TRUE)
    for (col in missed_numeric_cols) {
      if (col %in% names(df)) {
        sample_values <- unique(head(df[[col]], 10))
        cat(paste0(col, " - Sample values: ", paste(sample_values, collapse=", "), "\n"), 
            file = log_file, append = TRUE)
      }
    }
  }
  
  # Print summary
  cat("\nConversion summary:\n", file = log_file, append = TRUE)
  for (col in names(df)) {
    cat(paste0(col, ": ", class(df[[col]])[1], "\n"), file = log_file, append = TRUE)
  }
  
  return(df)
}

# Enhanced integrity checking function
enhanced_check_integrity <- function(df, df_name, expected_cols = NULL) {
  
  # Create the output directory if it doesn't exist
  output_dir <- here::here("docs", "data_exploration")
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Open a connection to write the summary text file
  summary_file <- file.path(output_dir, paste0("integrity_summary_", df_name, ".txt"))
  sink(summary_file)
  
  cat("------------------------------------------------------------\n")
  cat("Enhanced Integrity Check for:", df_name, "\n")
  
  if (is.null(df)) {
    cat("Data is NULL. Skipping.\n\n")
    sink()
    return(invisible(NULL))
  }
  
  # Basic Checks
  cat("Dimensions:", paste(dim(df), collapse = " x "), "\n")
  if (!is.null(expected_cols)) {
    missing_cols <- setdiff(expected_cols, names(df))
    if (length(missing_cols) > 0) {
      cat("WARNING: Missing columns in", df_name, ":", paste(missing_cols, collapse = ", "), "\n")
    } else {
      cat("All expected columns are present in", df_name, "\n")
    }
  }
  
  cat("\nSummary of column types in", df_name, ":\n")
  col_types <- sapply(df, function(x) class(x)[1])
  print(table(col_types))
  
  cat("\nDetailed summary of", df_name, ":\n")
  tryCatch({
    print(summary(df))
  }, error = function(e) {
    cat("Error generating summary: ", e$message, "\n")
  })
  
  # 1. Missing Value Analysis
  cat("\nMissing Value Analysis:\n")
  missing_counts <- sapply(df, function(x) sum(is.na(x)))
  missing_percents <- round(missing_counts / nrow(df) * 100, 2)
  missing_df <- data.frame(
    Column = names(missing_counts),
    Missing_Count = missing_counts,
    Missing_Percent = missing_percents,
    stringsAsFactors = FALSE
  )
  missing_df <- missing_df[order(-missing_df$Missing_Count), ]
  print(head(missing_df, 20))
  
  # 2. Unique values check for candidate ID columns
  id_cols <- grep("id$|customer|account", names(df), ignore.case = TRUE, value = TRUE)
  if (length(id_cols) > 0) {
    cat("\nUnique values check for potential ID columns:\n")
    # id_unique_counts <- sapply(df[, ..id_cols, drop = FALSE], function(x) length(unique(x)))
    
    if(is.data.table(df)) {
      id_unique_counts <- sapply(df[, id_cols, with = FALSE], function(x) length(unique(x)))
    } else {
      id_unique_counts <- sapply(df[, id_cols, drop = FALSE], function(x) length(unique(x)))
    }
    
    print(id_unique_counts)
  }
  
  # 3. Date Consistency Check if applicable
  date_cols <- grep("date$|time$", names(df), ignore.case = TRUE, value = TRUE)
  if (length(date_cols) >= 2) {
    cat("\nDate columns found:", paste(date_cols, collapse = ", "), "\n")
    
    # Check if we have start/end date pairs
    start_date_cols <- grep("start|begin", date_cols, ignore.case = TRUE, value = TRUE)
    end_date_cols <- grep("end|close|finish", date_cols, ignore.case = TRUE, value = TRUE)
    
    if (length(start_date_cols) > 0 && length(end_date_cols) > 0) {
      cat("\nDate consistency checks for start/end pairs:\n")
      
      for (start_col in start_date_cols) {
        for (end_col in end_date_cols) {
          cat("Checking", start_col, "vs", end_col, ":\n")
          
          # Extract columns and ensure they're date objects
          start_dates <- df[[start_col]]
          if (!inherits(start_dates, "Date")) {
            cat("  - Warning:", start_col, "is not a Date object (class:", class(start_dates)[1], ")\n")
            next
          }
          
          end_dates <- df[[end_col]]
          if (!inherits(end_dates, "Date")) {
            cat("  - Warning:", end_col, "is not a Date object (class:", class(end_dates)[1], ")\n")
            next
          }
          
          # Only compare where both dates are non-NA
          valid_idx <- which(!is.na(start_dates) & !is.na(end_dates))
          if (length(valid_idx) == 0) {
            cat("  - No records with both dates non-NA\n")
            next
          }
          
          # Check for inconsistencies
          invalid_idx <- valid_idx[which(start_dates[valid_idx] > end_dates[valid_idx])]
          
          if (length(invalid_idx) > 0) {
            invalid_pct <- round(length(invalid_idx) / length(valid_idx) * 100, 2)
            cat("  - Found", length(invalid_idx), "records (", invalid_pct, "%) where", 
                start_col, "is after", end_col, "\n")
          } else {
            cat("  - All dates are consistent\n")
          }
        }
      }
    }
  }
  
  cat("------------------------------------------------------------\n\n")
  sink()  # Close the text output connection
  
  # 4. Plot Generation ----------------------------------------------------------
  
  ## Missing Values Plot
  missing_df <- data.frame(
    Variable = names(missing_counts),
    Missing = as.numeric(missing_counts)
  )
  p_missing <- ggplot(missing_df, aes(x = reorder(Variable, -Missing), y = Missing)) +
    geom_bar(stat = "identity", fill = "steelblue") +
    coord_flip() +
    labs(title = paste("Missing Values in", df_name),
         x = "Variable", y = "Count of Missing Values") +
    theme_minimal()
  ggsave(filename = file.path(output_dir, paste0("missing_values_", df_name, ".png")),
         plot = p_missing, width = 8, height = 6)
  
  ## Outlier Detection Boxplots for numeric columns
  numeric_vars <- names(df)[sapply(df, is.numeric)]
  if (length(numeric_vars) > 0) {
    for (var in numeric_vars) {
      tryCatch({
        p_box <- ggplot(df, aes(y = .data[[var]])) +
          geom_boxplot(fill = "tomato") +
          labs(title = paste("Boxplot of", var, "in", df_name),
               y = var) +
          theme_minimal()
        ggsave(filename = file.path(output_dir, paste0("boxplot_", var, "_", df_name, ".png")),
               plot = p_box, width = 6, height = 4)
      }, error = function(e) {
        warning("Could not create boxplot for ", var, ": ", e$message)
      })
    }
  }
  
  message("Enhanced integrity check completed for ", df_name)
  message("Summary saved to: ", summary_file)
  message("Plots saved to: ", output_dir)
}

# Cleaning Functions for Each Dataset -------------------------------------

clean_opps_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning opportunities data...")
  
  # Fix corrupted date columns first before any other operations
  date_like_cols <- grep("date|time|on$", names(df), value = TRUE, ignore.case = TRUE)
  
  for (col in date_like_cols) {
    if (col %in% names(df)) {
      # Check if column has mixed types
      if (any(sapply(df[[col]], class) != sapply(df[[col]], class)[1], na.rm = TRUE)) {
        message("Fixing mixed types in column: ", col)
        # Convert to character first to normalize
        df[[col]] <- as.character(df[[col]])
      }

      # For POSIXct columns that might be corrupted
      if (inherits(df[[col]], "POSIXct") && any(is.na(df[[col]]))) {
        message("Fixing potentially corrupted POSIXct column: ", col)
        # Convert to character and back to POSIXct to clean
        temp_char <- as.character(df[[col]])
        df[[col]] <- as.POSIXct(temp_char)
      }
    }
  }

  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # Rest of your function continues from here...
  # 2. Standardize column names
  names(df) <- names(df) %>% 
    tolower() %>% 
    str_replace_all(" ", "_")
  
  # 3. Rename key columns
  if (all(c("id", "customer_id", "account", "start_date", "close_date") %in% names(df))) {
    df <- df %>%
      rename(
        to_id                = id,
        to_customer_id       = customer_id,
        to_customer_name     = account,
        to_start_date        = start_date,
        to_close_date        = close_date
      )
  }
  
  # Additional renames (function remains the same)
  possibly_rename <- function(df, old, new) {
    if (old %in% names(df)) {
      df <- df %>% rename(!!new := !!old)
    }
    return(df)
  }
  
  df <- possibly_rename(df, "sales_phase", "to_sales_phase")
  df <- possibly_rename(df, "opportunity_status", "to_opportunity_status")
  df <- possibly_rename(df, "expected_value", "to_expected_value")
  df <- possibly_rename(df, "product_category", "to_product_category")
  df <- possibly_rename(df, "probability", "to_probability")
  df <- possibly_rename(df, "weighted_value", "to_weighted_value")
  
  # 4. Define column type mappings
  numeric_cols <- c(
    "to_expected_value", 
    "price_of_competitor_machine", 
    "to_probability",
    "to_weighted_value", 
    "probability", 
    "weighted_value",
    "expected_value",
    "order_probability_for_trumpf"
  )
  
  # 5. Direct handling of key date columns that look like Excel numeric dates
  # The sample output shows these are numeric Excel dates
  date_columns_to_check <- c("to_start_date", "to_close_date", "expected_closure_date")
  # Try multiple methods for date conversion
  for (col in date_columns_to_check) {
    if (col %in% names(df) && !inherits(df[[col]], "Date")) {
      message("Processing ", col, " using multiple methods")
      
      # Method 1: Try Excel numeric date
      if (is.numeric(df[[col]]) || (is.character(df[[col]]) && all(grepl("^[0-9]+$", na.omit(df[[col]]))))) {
        temp <- suppressWarnings(as.numeric(df[[col]]))
        date_result <- as.Date(temp, origin = "1899-12-30")
        if (sum(!is.na(date_result)) > sum(!is.na(df[[col]]))/2) {
          df[[col]] <- date_result
          message("Converted ", col, " using Excel numeric date format")
          next
        }
      }
      
      # Method 2: Try common string formats
      if (is.character(df[[col]])) {
        date_result <- parse_date_time(df[[col]], orders = c("ymd", "dmy", "mdy"), quiet = TRUE)
        if (sum(!is.na(date_result)) > sum(!is.na(df[[col]]))/2) {
          df[[col]] <- as.Date(date_result)
          message("Converted ", col, " using string date format")
          next
        }
      }
      
      message("Failed to convert ", col, " to Date type")
    }
  }
  
  # 6. Handle other timestamps
  timestamp_cols <- c("changed_on", "created_on")
  for (col in timestamp_cols) {
    if (col %in% names(df)) {
      message("Processing ", col, " as Excel numeric timestamp")
      
      # First ensure it's numeric
      if (!is.numeric(df[[col]])) {
        df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
      }
      
      # If numeric conversion succeeded
      if (is.numeric(df[[col]])) {
        # Convert from Excel numeric date to R POSIXct (preserves time)
        fraction <- df[[col]] - floor(df[[col]])  # Extract the fractional part (time)
        hours <- round(fraction * 24, 6)  # Convert to hours (with rounding to avoid precision issues)
        
        # Convert to POSIXct
        dates <- as.Date(floor(df[[col]]), origin = "1899-12-30")
        timestamps <- as.POSIXct(dates) + hours * 3600  # Add seconds
        
        df[[col]] <- timestamps
        message("Converted ", col, " to POSIXct timestamp")
      }
    }
  }
  
  # 7. Define other date columns to process with the general converter
  date_cols <- grep("date$|time$|on$", names(df), value = TRUE)
  date_cols <- setdiff(date_cols, c(date_columns_to_check, timestamp_cols))  # Skip already converted
  
  factor_cols <- c("to_sales_phase", "to_opportunity_status", "to_product_category")
  
  # 8. Preprocess percentage columns
  percentage_cols <- c("probability", "to_probability", "order_probability_for_trumpf")
  for (col in percentage_cols) {
    if (col %in% names(df) && is.character(df[[col]])) {
      df[[col]] <- gsub("%", "", df[[col]])  # Remove percentage signs
      df[[col]] <- gsub("\\s+", "", df[[col]])  # Remove spaces
    }
  }
  
  # 9. Handle remaining type conversions with logging
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    date_cols = date_cols,
    factor_cols = factor_cols,
    df_name = "opps_data"
  )
  
  # 10. Derive additional date-based features if dates are valid
  if (all(c("to_start_date", "to_close_date") %in% names(df))) {
    message("Checking date conversions...")
    message("to_start_date class: ", class(df$to_start_date)[1])
    message("to_close_date class: ", class(df$to_close_date)[1])
    
    if (inherits(df$to_start_date, "Date")) {
      message("Deriving features from to_start_date...")
      df <- df %>%
        mutate(
          to_start_year = year(to_start_date),
          to_start_quarter = quarter(to_start_date),
          to_start_month = month(to_start_date)
        )
    }
    
    if (inherits(df$to_close_date, "Date")) {
      message("Deriving features from to_close_date...")
      df <- df %>%
        mutate(
          to_close_year = year(to_close_date),
          to_close_quarter = quarter(to_close_date),
          to_close_month = month(to_close_date)
        )
    }
    
    # Calculate duration if both dates are present and valid
    if (inherits(df$to_start_date, "Date") && inherits(df$to_close_date, "Date")) {
      message("Calculating duration...")
      df <- df %>%
        mutate(to_duration_in_days = as.numeric(to_close_date - to_start_date))
    }
  }
  
  ## This step seems corrupted as I get an issue with the `entry_date` column; however, I cannot find the creation of this column here.
  # 11. Clean text columns: trim whitespace
  # text_cols <- names(df)[sapply(df, is.character)]
  # if (length(text_cols) > 0) {
  #   df <- df %>%
  #     mutate(across(all_of(text_cols), ~str_squish(.)))
  # }
  
  # 11. Clean text columns: only apply to safe columns
  # text_cols <- names(df)[sapply(df, is.character)]
  # # Exclude any columns with date-like names
  # text_cols <- text_cols[!grepl("date|time|on$", text_cols, ignore.case = TRUE)]
  # if (length(text_cols) > 0) {
  #   for (col in text_cols) {
  #     # Process one column at a time to avoid issues
  #     tryCatch({
  #       df[[col]] <- str_squish(df[[col]])
  #     }, error = function(e) {
  #       message("Warning: Could not clean column ", col, ": ", e$message)
  #     })
  #   }
  # }
  
  # Process text columns individually with error checking
  text_cols <- names(df)[sapply(df, is.character)]
  # Exclude date-like columns
  text_cols <- text_cols[!grepl("date|time", text_cols, ignore.case = TRUE)]
  
  for (col in text_cols) {
    tryCatch({
      df[[col]] <- trimws(df[[col]])
    }, error = function(e) {
      message("Error cleaning text column ", col, ": ", e$message)
    })
  }
  
  # 12. Safe distinct operation
  # First convert to tibble to handle potential corrupted columns
  df <- as_tibble(df)
  
  # Make a safer version of distinct that handles errors
  if (all(c("to_id", "to_start_date", "to_close_date") %in% names(df))) {
    # Create clean ID for deduplication
    df$dedupe_id <- paste(df$to_id, as.character(df$to_start_date), 
                          as.character(df$to_close_date), sep = "|")
    df <- df[!duplicated(df$dedupe_id), ]
    df$dedupe_id <- NULL
  } else if ("to_id" %in% names(df)) {
    df <- df[!duplicated(df$to_id), ]
  }
  
  # 13. Check date consistency and flag issues
  if (all(c("to_start_date", "to_close_date") %in% names(df)) && 
      inherits(df$to_start_date, "Date") && inherits(df$to_close_date, "Date")) {
    
    # Count records with date inconsistencies
    inconsistent_dates <- sum(df$to_start_date > df$to_close_date, na.rm = TRUE)
    
    if (inconsistent_dates > 0) {
      message("Found ", inconsistent_dates, " records where start date is after close date")
      
      # Flag these records rather than removing them
      df <- df %>%
        mutate(date_inconsistency = ifelse(
          !is.na(to_start_date) & !is.na(to_close_date) & to_start_date > to_close_date,
          TRUE,
          FALSE
        ))
    }
  }
  
  # 14. Ensure customer_id is character for consistent joins
  if ("to_customer_id" %in% names(df)) {
    df$to_customer_id <- as.character(df$to_customer_id)
  }
  
  message("Cleaned opportunities data: ", nrow(df), " rows")
  return(df)
}

clean_quotes_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning quotes data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names: lowercase & replace spaces with underscores
  names(df) <- names(df) %>% 
    tolower() %>% 
    str_replace_all(" ", "_")
  
  # 3. Rename key columns with consistent naming
  rename_map <- c(
    id = "tq_id",
    progress = "tq_progress",
    status = "tq_status",
    description = "tq_description",
    account = "tq_customer",
    customer_id = "tq_customer_id",
    total = "tq_total_value",
    date = "tq_date",
    owner = "tq_owner",
    territory = "tq_territory",
    opportunity = "tq_opportunity",
    created_on = "tq_creation_date"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(rename_map)) {
    if (old_name %in% names(df)) {
      df <- df %>% rename(!!rename_map[old_name] := !!old_name)
    }
  }
  
  # 4. Define column type mappings
  numeric_cols <- grep("total|value|price|amount", names(df), value = TRUE)
  date_cols <- grep("date$|time$|on$", names(df), value = TRUE)
  factor_cols <- grep("status$|progress$|phase", names(df), value = TRUE)
  
  # 5. Handle type conversions with logging
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    date_cols = date_cols,
    factor_cols = factor_cols,
    df_name = "quotes_data"
  )
  
  # 6. Derive year from date if available
  if ("tq_date" %in% names(df) && inherits(df$tq_date, "Date")) {
    df <- df %>% mutate(tq_year = year(tq_date))
  }
  
  # 7. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 8. Remove duplicates
  if ("tq_id" %in% names(df) && "tq_date" %in% names(df)) {
    df <- df %>% distinct(tq_id, tq_date, .keep_all = TRUE)
  } else if ("tq_id" %in% names(df)) {
    df <- df %>% distinct(tq_id, .keep_all = TRUE)
  }
  
  # 9. Ensure customer_id is character for consistent joins
  if ("tq_customer_id" %in% names(df)) {
    df$tq_customer_id <- as.character(df$tq_customer_id)
  }
  
  message("Cleaned quotes data: ", nrow(df), " rows")
  return(df)
}

clean_sap_software <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning SAP software data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  # (Already done through janitor::clean_names in the loading function)
  
  # 3. Rename key columns using "ts_" prefix
  rename_map <- c(
    geschaftsj_periode = "ts_year_raw",
    gultig_ab = "ts_pur_date",
    geschaftsjahr = "ts_fin_year",
    land_auftraggeber = "ts_country_buyer",
    land = "ts_country",
    endkunde = "ts_customer_id",
    kunde = "ts_customer_name",
    x4_produkttype = "ts_product_type",
    x5_generation = "ts_generation",
    x6_produktoptionen = "ts_product_options",
    x7_lokale_sicht = "ts_product_details",
    material = "ts_material_num",
    position = "ts_position",
    umsatz = "ts_revenue"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(rename_map)) {
    if (old_name %in% names(df)) {
      df <- df %>% rename(!!rename_map[old_name] := !!old_name)
    }
  }
  
  # 4. Define column type mappings
  numeric_cols <- c("ts_revenue", "ts_year_raw", "ts_position")
  date_cols <- c("ts_pur_date")
  factor_cols <- c("ts_country", "ts_country_buyer", "ts_product_type", "ts_generation")
  
  # 5. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 6. Handle type conversions with special care for dates
  # For the purchase date field, try different formats
  if ("ts_pur_date" %in% names(df)) {
    # Check if it's a number that could be an Excel date
    if (all(grepl("^[0-9]+$", na.omit(df$ts_pur_date)))) {
      df$ts_pur_date <- as.Date(as.numeric(df$ts_pur_date), origin = "1899-12-30")
    } else {
      # Try common date formats
      df$ts_pur_date <- parse_date_time(df$ts_pur_date, 
                                        orders = c("ymd", "dmy", "mdy", "ydm"),
                                        quiet = TRUE)
      df$ts_pur_date <- as.Date(df$ts_pur_date)
    }
  }
  
  # Handle remaining type conversions
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    # date_cols handled above
    factor_cols = factor_cols,
    df_name = "sap_software"
  )
  
  # 7. Extract year from date if available
  if ("ts_pur_date" %in% names(df) && inherits(df$ts_pur_date, "Date")) {
    df <- df %>% mutate(ts_year = year(ts_pur_date))
  }
  
  # 8. Ensure customer_id is character for consistent joins
  if ("ts_customer_id" %in% names(df)) {
    df$ts_customer_id <- as.character(df$ts_customer_id)
  }
  
  # 9. Special handling for specific numeric fields with known issues
  if ("ts_position" %in% names(df) && !is.numeric(df$ts_position)) {
    # Try a more aggressive approach for ts_position
    numeric_values <- suppressWarnings(as.numeric(df$ts_position))
    valid_rows <- !is.na(numeric_values)
    if (sum(valid_rows) > 0) {
      df$ts_position <- numeric_values
    }
  }
  
  # 10. Remove duplicates
  df <- df %>% distinct()
  
  message("Cleaned SAP software data: ", nrow(df), " rows")
  return(df)
}

clean_sap_machines <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning SAP machines data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  names(df) <- names(df) %>% 
    tolower() %>% 
    str_replace_all(" ", "_")
  
  # 3. Rename key columns with "sm_" prefix
  rename_map <- c(
    bezeichnung = "sm_description",
    equipmentnummer = "sm_equipment_number",
    anschaffungsdatum = "sm_purchase_date",
    landerschlussel.x = "sm_country_code_x",
    material = "sm_material",
    name_1 = "sm_name",
    nummer_vertriebsauftrag = "sm_sales_order_number",
    vertriebssachbearbeiter = "sm_sales_rep",
    datum_rot = "sm_date_rot",
    hersteller_der_anlage_werk = "sm_manufacturer",
    fakturadatum = "sm_invoice_date",
    auftrageingangsdatum = "sm_order_entry_date",
    angelegt_am = "sm_created_at",
    warenausgangsdatum = "sm_shipping_date",
    submission = "sm_submission",
    nettowert = "sm_net_value",
    belegwahrung = "sm_document_currency",
    position_sd = "sm_position_sd",
    partnerrolle = "sm_partner_role",
    debitor = "sm_debtor",
    landerschlussel.y = "sm_country_code_y",
    adresskennzeichen = "sm_address_marker"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(rename_map)) {
    if (old_name %in% names(df)) {
      df <- df %>% rename(!!rename_map[old_name] := !!old_name)
    }
  }
  
  # 4. Define column type mappings
  numeric_cols <- c("sm_net_value", "sm_position_sd")
  date_cols <- c("sm_purchase_date", "sm_date_rot", "sm_invoice_date", 
                 "sm_order_entry_date", "sm_created_at", "sm_shipping_date")
  factor_cols <- c("sm_partner_role", "sm_document_currency", "sm_country_code_x", "sm_country_code_y")
  
  # 5. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 6. Handle date conversions
  # Most SAP dates need special handling
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      df[[date_col]] <- as.Date(
        parse_date_time(
          df[[date_col]], 
          orders = c("ymd", "dmy", "ymd HMS", "dmy HMS"),
          quiet = TRUE
        )
      )
    }
  }
  
  # Handle remaining type conversions
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    # date_cols handled above
    factor_cols = factor_cols,
    df_name = "sap_machines"
  )
  
  # 7. Derive date-based features
  if ("sm_purchase_date" %in% names(df) && inherits(df$sm_purchase_date, "Date")) {
    df <- df %>%
      mutate(
        purchase_year = year(sm_purchase_date),
        purchase_month = month(sm_purchase_date),
        purchase_day = day(sm_purchase_date)
      )
  }
  
  # 8. Ensure equipment_number is character
  if ("sm_equipment_number" %in% names(df)) {
    df$sm_equipment_number <- as.character(df$sm_equipment_number)
  }
  
  # 9. Remove duplicates (based on equipment number and purchase date if available)
  if (all(c("sm_equipment_number", "sm_purchase_date") %in% names(df))) {
    df <- df %>% distinct(sm_equipment_number, sm_purchase_date, .keep_all = TRUE)
  } else if ("sm_equipment_number" %in% names(df)) {
    df <- df %>% distinct(sm_equipment_number, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned SAP machines data: ", nrow(df), " rows")
  return(df)
}

clean_sis_installed_base <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning SiS installed base data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names (if not already done)
  df <- janitor::clean_names(df)
  
  # 3. Rename key columns with a "sis_" prefix
  rename_map <- c(
    equipment_number = "sis_equipment_number",
    equipment_type = "sis_equipment_type",
    equipment_group = "sis_equipment_group",
    country_code = "sis_country_code",
    start_up_1_date = "sis_start_date",
    start_up_2_date = "sis_install_date",
    reseller_warranty_start = "sis_reseller_warranty_start",
    reseller_warranty_end = "sis_reseller_warranty_end",
    customer_number = "sis_customer_number",
    customer_warranty_start = "sis_customer_warranty_start",
    customer_warranty_end = "sis_customer_warranty_end",
    summe_von_duration_years = "sis_duration_years"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(rename_map)) {
    if (old_name %in% names(df)) {
      df <- df %>% rename(!!rename_map[old_name] := !!old_name)
    }
  }
  
  # 4. Define column type mappings
  numeric_cols <- c("sis_duration_years")
  date_cols <- c("sis_start_date", "sis_install_date", "sis_reseller_warranty_start", 
                 "sis_reseller_warranty_end", "sis_customer_warranty_start", "sis_customer_warranty_end")
  factor_cols <- c("sis_equipment_type", "sis_equipment_group", "sis_country_code")
  
  # 5. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 6. Print some sample values of date columns to help with debugging
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      sample_vals <- head(na.omit(df[[date_col]]), 5)
      message("Sample ", date_col, " values: ", paste(sample_vals, collapse = ", "))
    }
  }
  
  # 7. Let the enhanced convert_types function handle all conversions
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    date_cols = date_cols,
    factor_cols = factor_cols,
    df_name = "sis_installed_base"
  )
  
  # 8. Derive date features - check first if conversion was successful
  if ("sis_start_date" %in% names(df)) {
    if (inherits(df$sis_start_date, "Date")) {
      message("sis_start_date was successfully converted to Date")
      df <- df %>%
        mutate(
          construction_year = year(sis_start_date),
          construction_month = month(sis_start_date),
          construction_day = day(sis_start_date)
        )
    } else {
      # Try one more direct conversion method
      message("Attempting fallback date conversion for sis_start_date")
      df$sis_start_date <- as.Date(df$sis_start_date)
      
      if (inherits(df$sis_start_date, "Date")) {
        df <- df %>%
          mutate(
            construction_year = year(sis_start_date),
            construction_month = month(sis_start_date),
            construction_day = day(sis_start_date)
          )
      } else {
        message("Failed to convert sis_start_date to Date type")
      }
    }
  }
  
  if ("sis_install_date" %in% names(df)) {
    if (inherits(df$sis_install_date, "Date")) {
      message("sis_install_date was successfully converted to Date")
      df <- df %>%
        mutate(
          installation_year = year(sis_install_date),
          installation_month = month(sis_install_date),
          installation_day = day(sis_install_date)
        )
    } else {
      # Try one more direct conversion method
      message("Attempting fallback date conversion for sis_install_date")
      df$sis_install_date <- as.Date(df$sis_install_date)
      
      if (inherits(df$sis_install_date, "Date")) {
        df <- df %>%
          mutate(
            installation_year = year(sis_install_date),
            installation_month = month(sis_install_date),
            installation_day = day(sis_install_date)
          )
      } else {
        message("Failed to convert sis_install_date to Date type")
      }
    }
  }
  
  # 9. Ensure customer_number is character
  if ("sis_customer_number" %in% names(df)) {
    df$sis_customer_number <- as.character(df$sis_customer_number)
  }
  
  # 10. Ensure equipment_number is character
  if ("sis_equipment_number" %in% names(df)) {
    df$sis_equipment_number <- as.character(df$sis_equipment_number)
  }
  
  # 11. Remove duplicates
  if ("sis_equipment_number" %in% names(df)) {
    df <- df %>% distinct(sis_equipment_number, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned SiS installed base data: ", nrow(df), " rows")
  return(df)
}

clean_sis_cases <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning SiS cases data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names (if not already done)
  df <- janitor::clean_names(df)
  
  # 3. Rename key columns with "sc_" prefix
  rename_map <- c(
    case_number = "sc_case_number",
    case_type = "sc_case_type",
    priority = "sc_priority",
    equipment_number = "sc_equipment_number",
    nsc_status = "sc_nsc_status",
    organization_of_editor = "sc_org_editor",
    group_of_editor = "sc_group_editor",
    customer_number = "sc_customer_number",
    customer_name = "sc_customer_name",
    organization_of_creator = "sc_org_creator",
    cw = "sc_cw",  # Calendar week
    solved_inhouse = "sc_solved_inhouse",
    count_of_missions = "sc_count_of_missions",
    escalation = "sc_escalation",
    reaction_time = "sc_reaction_time",
    summe_von_delayed_callbacks = "sc_delayed_callbacks",
    case_billed = "sc_case_billed"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(rename_map)) {
    if (old_name %in% names(df)) {
      df <- df %>% rename(!!rename_map[old_name] := !!old_name)
    }
  }
  
  # 4. Define column type mappings
  numeric_cols <- c("sc_reaction_time", "sc_delayed_callbacks", "sc_count_of_missions")
  logical_cols <- c("sc_solved_inhouse", "sc_escalation", "sc_case_billed")
  factor_cols <- c("sc_case_type", "sc_priority", "sc_nsc_status")
  
  # 5. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 6. Handle specific conversions for the calendar week field
  if ("sc_cw" %in% names(df)) {
    # Print sample values for debugging
    cw_samples <- head(na.omit(df$sc_cw), 5)
    message("Sample calendar week values: ", paste(cw_samples, collapse = ", "))
    
    # Extract year and calendar week from the CW field
    # Format should be like "2020/52" or "2020-52"
    df <- df %>%
      mutate(
        case_year = suppressWarnings(as.numeric(str_extract(sc_cw, "^[0-9]{4}"))),
        case_calendar_week = suppressWarnings(as.numeric(str_extract(sc_cw, "[0-9]{1,2}$")))
      )
  }
  
  # 7. Special handling for logical columns
  # Check sample values for logical columns
  for (log_col in logical_cols) {
    if (log_col %in% names(df)) {
      log_samples <- head(na.omit(df[[log_col]]), 5)
      message("Sample ", log_col, " values: ", paste(log_samples, collapse = ", "))
      
      # Try to identify common patterns for this specific dataset
      if (all(df[[log_col]] %in% c("Y", "N", "", NA), na.rm = TRUE)) {
        message("Converting ", log_col, " from Y/N format to logical")
        df[[log_col]] <- df[[log_col]] == "Y"
      } else if (all(df[[log_col]] %in% c("Yes", "No", "", NA), na.rm = TRUE)) {
        message("Converting ", log_col, " from Yes/No format to logical")
        df[[log_col]] <- df[[log_col]] == "Yes"
      } else if (all(df[[log_col]] %in% c("TRUE", "FALSE", "", NA), na.rm = TRUE)) {
        message("Converting ", log_col, " from TRUE/FALSE text to logical")
        df[[log_col]] <- df[[log_col]] == "TRUE"
      } else if (all(df[[log_col]] %in% c("1", "0", "", NA), na.rm = TRUE)) {
        message("Converting ", log_col, " from 1/0 format to logical")
        df[[log_col]] <- df[[log_col]] == "1"
      }
    }
  }
  
  # 8. Handle remaining type conversions
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    logical_cols = logical_cols,
    factor_cols = factor_cols,
    df_name = "sis_cases"
  )
  
  # 9. Ensure customer_number is character
  if ("sc_customer_number" %in% names(df)) {
    df$sc_customer_number <- as.character(df$sc_customer_number)
  }
  
  # 10. Ensure equipment_number is character
  if ("sc_equipment_number" %in% names(df)) {
    df$sc_equipment_number <- as.character(df$sc_equipment_number)
  }
  
  # 11. Check conversion success and report
  for (log_col in logical_cols) {
    if (log_col %in% names(df)) {
      if (is.logical(df[[log_col]])) {
        message("Successfully converted ", log_col, " to logical type")
      } else {
        message("Failed to convert ", log_col, " to logical type, still ", class(df[[log_col]])[1])
      }
    }
  }
  
  for (num_col in numeric_cols) {
    if (num_col %in% names(df)) {
      if (is.numeric(df[[num_col]])) {
        message("Successfully converted ", num_col, " to numeric type")
      } else {
        message("Failed to convert ", num_col, " to numeric type, still ", class(df[[num_col]])[1])
      }
    }
  }
  
  # 12. Remove duplicates
  if ("sc_case_number" %in% names(df)) {
    df <- df %>% distinct(sc_case_number, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned SiS cases data: ", nrow(df), " rows")
  return(df)
}

clean_sis_missions <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning SiS missions data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names (if not already done)
  df <- janitor::clean_names(df)
  
  # 3. Rename key columns with "sm_" prefix for SiS Missions (to distinguish from SAP Machines)
  # Without seeing the actual column names, we'll leave this for now
  
  # 4. Define column type mappings
  # These would need to be updated based on your actual column names
  numeric_cols <- c("waiting_time_days", "working_time_h", "travel_time_h")
  logical_cols <- c("missions_delayed")
  factor_cols <- c("priority", "mission_type", "billing_type")
  
  # 5. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 6. Handle specific conversions for the calendar week field
  if ("cw" %in% names(df)) {
    # Extract year and calendar week from the CW field
    df <- df %>%
      mutate(
        mission_year = as.numeric(str_extract(cw, "^[0-9]{4}")),
        mission_calendar_week = as.numeric(str_extract(cw, "[0-9]{1,2}$"))
      )
  }
  
  # Special handling for travel_time_h if it exists but isn't numeric
  if ("travel_time_h" %in% names(df) && is.character(df$travel_time_h)) {
    df$travel_time_h <- as.numeric(gsub("[^0-9\\.-]", "", df$travel_time_h))
  }
  
  # Handle remaining type conversions
  df <- convert_types(
    df = df, 
    numeric_cols = numeric_cols,
    logical_cols = logical_cols,
    factor_cols = factor_cols,
    df_name = "sis_missions"
  )
  
  # 7. Ensure mission_id and case number are characters
  if ("mission_id" %in% names(df)) {
    df$mission_id <- as.character(df$mission_id)
  }
  
  if ("case_number" %in% names(df)) {
    df$case_number <- as.character(df$case_number)
  }
  
  # 8. Remove duplicates
  if ("mission_id" %in% names(df)) {
    df <- df %>% distinct(mission_id, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned SiS missions data: ", nrow(df), " rows")
  return(df)
}

clean_customer_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning customer data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names (if not already done)
  df <- janitor::clean_names(df)
  
  # 3. Rename key columns
  if (all(c("account_id", "sap_id", "name") %in% names(df))) {
    df <- df %>%
      rename(
        customer_account_id = account_id,
        customer_sap_id = sap_id,
        customer_name = name
      )
  }
  
  # 4. Clean text columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 5. Ensure ID columns are character
  if ("customer_account_id" %in% names(df)) {
    df$customer_account_id <- as.character(df$customer_account_id)
  }
  
  if ("customer_sap_id" %in% names(df)) {
    df$customer_sap_id <- as.character(df$customer_sap_id)
  }
  
  # 6. Remove duplicates
  if ("customer_account_id" %in% names(df)) {
    df <- df %>% distinct(customer_account_id, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned customer data: ", nrow(df), " rows")
  return(df)
}

clean_contacts_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning contacts data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  df <- janitor::clean_names(df)
  
  # 3. Clean character columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 4. Handle date columns
  date_cols <- grep("date|created|changed", names(df), value = TRUE)
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      df[[date_col]] <- as.Date(
        parse_date_time(
          df[[date_col]], 
          orders = c("ymd", "dmy", "ymd HMS", "dmy HMS"),
          quiet = TRUE
        )
      )
    }
  }
  
  # 5. Ensure account_id is character
  if ("account_id" %in% names(df)) {
    df$account_id <- as.character(df$account_id)
  }
  
  # 6. Create a contact quality score column
  # Count non-NA values across all columns for each row
  if (ncol(df) > 0) {
    df <- df %>%
      mutate(data_quality_score = rowSums(!is.na(.)) / ncol(.))
  }
  
  # 7. Remove duplicates based on ID and account
  id_col <- grep("^id$|contact_id", names(df), value = TRUE)
  if (length(id_col) > 0 && "account_id" %in% names(df)) {
    df <- df %>% distinct(!!sym(id_col[1]), account_id, .keep_all = TRUE)
  } else if (length(id_col) > 0) {
    df <- df %>% distinct(!!sym(id_col[1]), .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned contacts data: ", nrow(df), " rows")
  return(df)
}

clean_meetings_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning meetings data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  df <- janitor::clean_names(df)
  
  # 3. Clean character columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 4. Identify datetime columns
  date_cols <- grep("date|time|created|changed", names(df), value = TRUE)
  
  # Show some samples for debugging
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      sample_vals <- head(na.omit(df[[date_col]]), 5)
      message("Sample ", date_col, " values: ", paste(sample_vals, collapse = ", "))
    }
  }
  
  # 5. Use the improved convert_types function to handle dates
  df <- convert_types(
    df = df,
    date_cols = date_cols,
    df_name = "meetings_data"
  )
  
  # 6. Calculate meeting duration if start and end times are available
  if (all(c("start_date_time", "end_date_time") %in% names(df)) &&
      (inherits(df$start_date_time, "POSIXct") || inherits(df$start_date_time, "Date")) &&
      (inherits(df$end_date_time, "POSIXct") || inherits(df$end_date_time, "Date"))) {
    
    message("Calculating meeting durations...")
    df <- df %>%
      mutate(
        meeting_duration_minutes = as.numeric(
          difftime(end_date_time, start_date_time, units = "mins")
        )
      )
    
    # Flag suspicious durations (negative or very long)
    df <- df %>%
      mutate(
        duration_suspicious = meeting_duration_minutes < 0 | meeting_duration_minutes > 480
      )
    
    message("Durations calculated successfully")
  } else {
    message("Could not calculate meeting durations due to missing or unconverted datetime fields")
  }
  
  # 7. Extract date components if start time is available
  if ("start_date_time" %in% names(df) && 
      (inherits(df$start_date_time, "POSIXct") || inherits(df$start_date_time, "Date"))) {
    
    message("Extracting date components...")
    
    # Safe function to extract date components that handles both Date and POSIXct
    safe_extract <- function(date_obj, extractor_fn) {
      tryCatch({
        extractor_fn(date_obj)
      }, error = function(e) {
        # If error, return NA
        rep(NA_integer_, length(date_obj))
      })
    }
    
    df <- df %>%
      mutate(
        meeting_starting_year = safe_extract(start_date_time, year),
        meeting_starting_month = safe_extract(start_date_time, month),
        meeting_starting_day = safe_extract(start_date_time, day)
      )
    
    # For time and weekday, handle POSIXct specifically
    if (inherits(df$start_date_time, "POSIXct")) {
      df <- df %>%
        mutate(
          meeting_starting_time = format(start_date_time, format = "%H:%M:%S"),
          meeting_weekday = weekdays(start_date_time)
        )
    } else {
      # For Date objects, time will be 00:00:00
      df <- df %>%
        mutate(
          meeting_starting_time = "00:00:00",
          meeting_weekday = weekdays(start_date_time)
        )
    }
    
    message("Date components extracted successfully")
  } else {
    message("Could not extract date components due to missing or unconverted datetime field")
  }
  
  # 8. Ensure account_id is character
  if ("account_id" %in% names(df)) {
    df$account_id <- as.character(df$account_id)
  }
  
  # 9. Remove duplicates
  if ("id" %in% names(df)) {
    df <- df %>% distinct(id, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned meetings data: ", nrow(df), " rows")
  return(df)
}

clean_mails_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning mails data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  df <- janitor::clean_names(df)
  
  # 3. Clean character columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 4. Handle date columns
  date_cols <- grep("date|time|sent|created|changed", names(df), value = TRUE)
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      df[[date_col]] <- parse_date_time(
        df[[date_col]], 
        orders = c("ymd HMS", "dmy HMS", "ymd", "dmy"),
        quiet = TRUE
      )
    }
  }
  
  # 5. Extract date components from sent date
  if ("sent_on" %in% names(df)) {
    df <- df %>%
      mutate(
        sent_year = year(sent_on),
        sent_month = month(sent_on),
        sent_day = day(sent_on),
        sent_time = format(sent_on, format = "%H:%M:%S"),
        sent_weekday = weekdays(sent_on),
        sent_hour = hour(sent_on),
        is_business_hours = sent_hour >= 8 & sent_hour <= 17
      )
  }
  
  # 6. Add message analysis fields
  if ("subject" %in% names(df)) {
    df <- df %>%
      mutate(
        subject_length = nchar(subject),
        has_attachment = str_detect(tolower(subject), "attach|enclosed|pdf|doc|xls"),
        is_reply = str_detect(tolower(subject), "^re:|^fw:|^fwd:"),
        subject_clean = str_remove_all(subject, "^(re:|fw:|fwd:)\\s*")
      )
  }
  
  # 7. Ensure account_id is character
  if ("account_id" %in% names(df)) {
    df$account_id <- as.character(df$account_id)
  }
  
  # 8. Remove duplicates
  if ("id" %in% names(df)) {
    df <- df %>% distinct(id, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned mails data: ", nrow(df), " rows")
  return(df)
}

clean_other_act_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning other activities data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  df <- janitor::clean_names(df)
  
  # 3. Clean character columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 4. Handle date columns
  date_cols <- grep("date|time|created|changed", names(df), value = TRUE)
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      df[[date_col]] <- parse_date_time(
        df[[date_col]], 
        orders = c("ymd HMS", "dmy HMS", "ymd", "dmy"),
        quiet = TRUE
      )
    }
  }
  
  # 5. Calculate activity duration if start and end times are available
  if (all(c("start_date_time", "end_date_time") %in% names(df))) {
    df <- df %>%
      mutate(
        activity_duration_minutes = as.numeric(
          difftime(end_date_time, start_date_time, units = "mins")
        )
      )
    
    # Flag suspicious durations
    df <- df %>%
      mutate(
        duration_suspicious = activity_duration_minutes < 0 | activity_duration_minutes > 480
      )
  }
  
  # 6. Extract date components from start time
  if ("start_date_time" %in% names(df)) {
    df <- df %>%
      mutate(
        activity_year = year(start_date_time),
        activity_month = month(start_date_time),
        activity_day = day(start_date_time),
        activity_weekday = weekdays(start_date_time),
        activity_hour = hour(start_date_time),
        is_business_hours = activity_hour >= 9 & activity_hour <= 17
      )
  }
  
  # 7. Ensure account_id is character
  if ("account_id" %in% names(df)) {
    df$account_id <- as.character(df$account_id)
  }
  
  # 8. Remove duplicates
  if ("id" %in% names(df)) {
    df <- df %>% distinct(id, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned other activities data: ", nrow(df), " rows")
  return(df)
}

clean_registered_products <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning registered products data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Standardize column names
  df <- janitor::clean_names(df)
  
  # 3. Clean character columns
  text_cols <- names(df)[sapply(df, is.character)]
  if (length(text_cols) > 0) {
    df <- df %>%
      mutate(across(all_of(text_cols), ~str_squish(.)))
  }
  
  # 4. Handle date columns
  date_cols <- grep("date|time|warranty|shipping|completion|created", names(df), value = TRUE)
  for (date_col in date_cols) {
    if (date_col %in% names(df)) {
      df[[date_col]] <- as.Date(
        parse_date_time(
          df[[date_col]], 
          orders = c("ymd", "dmy", "ymd HMS", "dmy HMS"),
          quiet = TRUE
        )
      )
    }
  }
  
  # 5. Extract date components from reference date
  if ("reference_date_reg_prod" %in% names(df) && 
      inherits(df$reference_date_reg_prod, "Date")) {
    df <- df %>%
      mutate(
        reference_year = year(reference_date_reg_prod),
        reference_month = month(reference_date_reg_prod),
        reference_day = day(reference_date_reg_prod)
      )
  }
  
  # 6. Extract date components from warranty end date
  if ("warranty_end_date_reg_prod" %in% names(df) && 
      inherits(df$warranty_end_date_reg_prod, "Date")) {
    df <- df %>%
      mutate(
        warranty_end_year = year(warranty_end_date_reg_prod),
        warranty_end_month = month(warranty_end_date_reg_prod),
        warranty_end_day = day(warranty_end_date_reg_prod)
      )
    
    # Add warranty status
    df <- df %>%
      mutate(
        warranty_status = case_when(
          is.na(warranty_end_date_reg_prod) ~ "unknown",
          warranty_end_date_reg_prod < Sys.Date() ~ "expired",
          warranty_end_date_reg_prod >= Sys.Date() ~ "active"
        ),
        
        warranty_days_remaining = as.numeric(
          difftime(warranty_end_date_reg_prod, Sys.Date(), units = "days")
        )
      )
  }
  
  # 7. Extract date components from shipping date
  if ("shipping_date_reg_prod" %in% names(df) && 
      inherits(df$shipping_date_reg_prod, "Date")) {
    df <- df %>%
      mutate(
        shipping_year = year(shipping_date_reg_prod),
        shipping_month = month(shipping_date_reg_prod),
        shipping_day = day(shipping_date_reg_prod)
      )
  }
  
  # 8. Extract date components from completion date
  if ("completion_date_reg_prod" %in% names(df) && 
      inherits(df$completion_date_reg_prod, "Date")) {
    df <- df %>%
      mutate(
        completion_year = year(completion_date_reg_prod),
        completion_month = month(completion_date_reg_prod),
        completion_day = day(completion_date_reg_prod)
      )
  }
  
  # 9. Ensure account_id is character
  account_id_col <- grep("account_id|customer_id", names(df), value = TRUE)
  if (length(account_id_col) > 0) {
    df[[account_id_col[1]]] <- as.character(df[[account_id_col[1]]])
  }
  
  # 10. Ensure serial_id is character
  if ("serial_id_reg_prod" %in% names(df)) {
    df$serial_id_reg_prod <- as.character(df$serial_id_reg_prod)
  }
  
  # 11. Remove duplicates
  if ("id_reg_prod" %in% names(df)) {
    df <- df %>% distinct(id_reg_prod, .keep_all = TRUE)
  } else if ("serial_id_reg_prod" %in% names(df)) {
    df <- df %>% distinct(serial_id_reg_prod, .keep_all = TRUE)
  } else {
    df <- df %>% distinct()
  }
  
  message("Cleaned registered products data: ", nrow(df), " rows")
  return(df)
}

clean_survey_data <- function(df) {
  # Exit early if dataframe is NULL
  if (is.null(df)) return(NULL)
  
  message("Cleaning survey data...")
  
  # 1. Remove entirely empty columns
  df <- df %>% select_if(~ !all(is.na(.)))
  
  # 2. Clean column names (remove \r\n, question text, and scale descriptions)
  cleaned_names <- names(df) %>%
    str_replace_all("\r\n", " ") %>%             # Remove line breaks
    str_replace_all("\\?.*$", "") %>%            # Remove question marks and everything after
    str_replace_all("\\([^)]*\\)", "") %>%       # Remove parentheses and their content
    str_trim() %>%                               # Remove leading/trailing whitespace
    make_clean_names()                           # Convert to snake_case
  
  colnames(df) <- cleaned_names
  
  # 3. Create short column names mapping for easier analysis
  col_mapping <- list(
    software_generation = "software_from_sap",
    core_competency = "is_the_offering_close_to_trump_fs_core_competency",
    hardware_proximity = "is_the_offering_close_to_hardware",
    performance_improvement = "after_implementation_how_much_do_trumpf_offerings_performance_improve",
    customer_lock_in = "after_implementation_how_strong_is_a_customers_lock_in_to_trumpf",
    investment_level = "how_long_does_it_take_for_customers_to_use_the_software_after_the_buying_decision",
    implementation_time = "how_long_does_it_take_for_customers_to_use_the_software_after_the_buying_decision",
    explanation_needed = "how_much_explanation_does_the_software_need",
    competitor_count = "how_many_competitor_products_do_exist",
    competitor_quality = "how_would_you_rate_the_competitions_best_alternative",
    non_trumpf_integration = "how_easy_can_non_trumpf_offerings_be_integrated_with_the_software",
    non_trumpf_functionality = "how_would_you_rate_the_functional_scope_of_the_software_applied_to_non_trumpf_offerings"
  )
  
  # Apply renaming only for columns that exist
  for (old_name in names(col_mapping)) {
    if (col_mapping[[old_name]] %in% names(df)) {
      df <- df %>% rename(!!old_name := !!col_mapping[[old_name]])
    }
  }
  
  # 4. Convert Likert scale columns to ordered factors
  # Find all columns that might be Likert scales (numeric 1-7)
  likert_candidates <- setdiff(names(df), "software_generation")
  
  for (col in likert_candidates) {
    if (col %in% names(df)) {
      # Check if the column values are consistent with Likert scale (1-7)
      unique_vals <- unique(na.omit(df[[col]]))
      if (all(unique_vals %in% 1:7) || all(unique_vals %in% as.character(1:7))) {
        # Convert to numeric first, then to ordered factor
        df[[col]] <- as.numeric(df[[col]])
        df[[col]] <- factor(df[[col]], levels = 1:7, ordered = TRUE)
      }
    }
  }
  
  # 5. Identify actual Likert scale columns
  likert_cols <- names(df)[sapply(df, is.ordered)]
  
  # 6. Add derived metrics
  numeric_likert <- lapply(df[likert_cols], as.numeric)
  numeric_likert_df <- as.data.frame(numeric_likert)
  
  # Only add these if we have the required columns
  if (all(c("core_competency", "hardware_proximity") %in% names(numeric_likert_df))) {
    df$core_alignment_score <- numeric_likert_df$core_competency + numeric_likert_df$hardware_proximity
  }
  
  if (all(c("non_trumpf_integration", "non_trumpf_functionality") %in% names(numeric_likert_df))) {
    df$integration_score <- numeric_likert_df$non_trumpf_integration + numeric_likert_df$non_trumpf_functionality
  }
  
  if (all(c("competitor_count", "competitor_quality") %in% names(numeric_likert_df))) {
    df$competition_pressure <- numeric_likert_df$competitor_count + numeric_likert_df$competitor_quality
  }
  
  if (all(c("implementation_time", "explanation_needed") %in% names(numeric_likert_df))) {
    df$implementation_complexity <- numeric_likert_df$implementation_time + numeric_likert_df$explanation_needed
  }
  
  # 7. Add response quality indicators
  if (length(likert_cols) > 1) {
    # Flag straight-line responses
    df$straight_line_responses <- apply(df[likert_cols], 1, function(x) {
      all(x == x[1], na.rm = TRUE)
    })
    
    # Calculate response range
    df$response_range <- apply(numeric_likert_df, 1, function(x) {
      diff(range(x, na.rm = TRUE))
    })
    
    # Flag low variance responses
    df$low_variance_responses <- df$response_range <= 1
  }
  
  message("Cleaned survey data: ", nrow(df), " rows")
  return(df)
}

# Load Interim Data ----

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
# survey_data          <- safe_readRDS(here::here("data", "interim", "survey_data.rds"))

# Run Integrity Checks ----

# First create log directory
dir.create(here::here("logs"), showWarnings = FALSE, recursive = TRUE)

# Run integrity checks on raw data
enhanced_check_integrity(opps_data,           "opps_data_raw")
enhanced_check_integrity(quotes_data,         "quotes_data_raw")
enhanced_check_integrity(sap_software_data,   "sap_software_data_raw")
enhanced_check_integrity(sap_machines_data,   "sap_machines_data_raw")
enhanced_check_integrity(sis_installed_base,  "sis_installed_base_raw")
enhanced_check_integrity(sis_cases,           "sis_cases_raw")
enhanced_check_integrity(sis_missions,        "sis_missions_raw")
enhanced_check_integrity(customer_data,       "customer_data_raw")
enhanced_check_integrity(contacts_data,       "contacts_data_raw")
enhanced_check_integrity(meetings_data,       "meetings_data_raw")
enhanced_check_integrity(mails_data,          "mails_data_raw")
enhanced_check_integrity(other_act_data,      "other_act_data_raw")
enhanced_check_integrity(registered_products, "registered_products_raw")
# enhanced_check_integrity(survey_data,         "survey_data_raw")

# Clean Data

opps_data_cleaned           <- clean_opps_data(opps_data)
quotes_data_cleaned         <- clean_quotes_data(quotes_data)
sap_software_data_cleaned   <- clean_sap_software(sap_software_data)
sap_machines_data_cleaned   <- clean_sap_machines(sap_machines_data)
sis_installed_base_cleaned  <- clean_sis_installed_base(sis_installed_base)
sis_cases_cleaned           <- clean_sis_cases(sis_cases)
sis_missions_cleaned        <- clean_sis_missions(sis_missions)
customer_data_cleaned       <- clean_customer_data(customer_data)
contacts_data_cleaned       <- clean_contacts_data(contacts_data)
meetings_data_cleaned       <- clean_meetings_data(meetings_data)
mails_data_cleaned          <- clean_mails_data(mails_data)
other_act_data_cleaned      <- clean_other_act_data(other_act_data)
registered_products_cleaned <- clean_registered_products(registered_products)
# survey_data_cleaned         <- clean_survey_data(survey_data)

# Run Integrity Checks on Cleaned Data

enhanced_check_integrity(opps_data_cleaned,           "opps_data_cleaned")
enhanced_check_integrity(quotes_data_cleaned,         "quotes_data_cleaned")
enhanced_check_integrity(sap_software_data_cleaned,   "sap_software_data_cleaned")
enhanced_check_integrity(sap_machines_data_cleaned,   "sap_machines_data_cleaned")
enhanced_check_integrity(sis_installed_base_cleaned,  "sis_installed_base_cleaned")
enhanced_check_integrity(sis_cases_cleaned,           "sis_cases_cleaned")
enhanced_check_integrity(sis_missions_cleaned,        "sis_missions_cleaned")
enhanced_check_integrity(customer_data_cleaned,       "customer_data_cleaned")
enhanced_check_integrity(contacts_data_cleaned,       "contacts_data_cleaned")
enhanced_check_integrity(meetings_data_cleaned,       "meetings_data_cleaned")
enhanced_check_integrity(mails_data_cleaned,          "mails_data_cleaned")
enhanced_check_integrity(other_act_data_cleaned,      "other_act_data_cleaned")
enhanced_check_integrity(registered_products_cleaned, "registered_products_cleaned")
# enhanced_check_integrity(survey_data_cleaned,         "survey_data_cleaned")

# Save Cleaned Data to Processed Folder

processed_dir <- here::here("data", "processed")
if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)

if (!is.null(opps_data_cleaned))           saveRDS(opps_data_cleaned,           file = file.path(processed_dir, "opps_data_cleaned.rds"))
if (!is.null(quotes_data_cleaned))         saveRDS(quotes_data_cleaned,         file = file.path(processed_dir, "quotes_data_cleaned.rds"))
if (!is.null(sap_software_data_cleaned))   saveRDS(sap_software_data_cleaned,   file = file.path(processed_dir, "sap_software_cleaned.rds"))
if (!is.null(sap_machines_data_cleaned))   saveRDS(sap_machines_data_cleaned,   file = file.path(processed_dir, "sap_machines_cleaned.rds"))
if (!is.null(sis_installed_base_cleaned))  saveRDS(sis_installed_base_cleaned,  file = file.path(processed_dir, "sis_installed_base_cleaned.rds"))
if (!is.null(sis_cases_cleaned))           saveRDS(sis_cases_cleaned,           file = file.path(processed_dir, "sis_cases_cleaned.rds"))
if (!is.null(sis_missions_cleaned))        saveRDS(sis_missions_cleaned,        file = file.path(processed_dir, "sis_missions_cleaned.rds"))
if (!is.null(customer_data_cleaned))       saveRDS(customer_data_cleaned,       file = file.path(processed_dir, "customer_data_cleaned.rds"))
if (!is.null(contacts_data_cleaned))       saveRDS(contacts_data_cleaned,       file = file.path(processed_dir, "contacts_data_cleaned.rds"))
if (!is.null(meetings_data_cleaned))       saveRDS(meetings_data_cleaned,       file = file.path(processed_dir, "meetings_data_cleaned.rds"))
if (!is.null(mails_data_cleaned))          saveRDS(mails_data_cleaned,          file = file.path(processed_dir, "mails_data_cleaned.rds"))
if (!is.null(other_act_data_cleaned))      saveRDS(other_act_data_cleaned,      file = file.path(processed_dir, "other_act_data_cleaned.rds"))
if (!is.null(registered_products_cleaned)) saveRDS(registered_products_cleaned, file = file.path(processed_dir, "registered_products_cleaned.rds"))
# if (!is.null(survey_data_cleaned))         saveRDS(survey_data_cleaned,         file = file.path(processed_dir, "survey_data_cleaned.rds"))

message("All data cleaned and saved in 'data/processed'!")

# Main Execution

main <- function() {
  # Log start time for performance tracking
  start_time <- Sys.time()
  message("Starting data cleaning process at ", format(start_time))
  
  # Print memory usage before cleaning
  message("Memory usage before cleaning: ", format(object.size(x = lapply(ls(envir = .GlobalEnv), get)), units = "Mb"))
  
  # Create a list of data frames and their names for cleaner code
  data_frames <- list(
    list(df = opps_data, name = "opps_data", clean_fn = clean_opps_data),
    list(df = quotes_data, name = "quotes_data", clean_fn = clean_quotes_data),
    list(df = sap_software_data, name = "sap_software_data", clean_fn = clean_sap_software),
    list(df = sap_machines_data, name = "sap_machines_data", clean_fn = clean_sap_machines),
    list(df = sis_installed_base, name = "sis_installed_base", clean_fn = clean_sis_installed_base),
    list(df = sis_cases, name = "sis_cases", clean_fn = clean_sis_cases),
    list(df = sis_missions, name = "sis_missions", clean_fn = clean_sis_missions),
    list(df = customer_data, name = "customer_data", clean_fn = clean_customer_data),
    list(df = contacts_data, name = "contacts_data", clean_fn = clean_contacts_data),
    list(df = meetings_data, name = "meetings_data", clean_fn = clean_meetings_data),
    list(df = mails_data, name = "mails_data", clean_fn = clean_mails_data),
    list(df = other_act_data, name = "other_act_data", clean_fn = clean_other_act_data),
    list(df = registered_products, name = "registered_products", clean_fn = clean_registered_products)#,
    # list(df = survey_data, name = "survey_data", clean_fn = clean_survey_data)
  )
  
  # Create processed directory
  processed_dir <- here::here("data", "processed")
  if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)
  
  # Process each data frame with error handling
  results <- list()
  
  for (item in data_frames) {
    if (!is.null(item$df)) {
      tryCatch({
        # Run integrity check on raw data
        enhanced_check_integrity(item$df, paste0(item$name, "_raw"))
        
        # Clean the data
        message("\nCleaning ", item$name, "...")
        cleaned_df <- item$clean_fn(item$df)
        
        # Run integrity check on cleaned data
        enhanced_check_integrity(cleaned_df, paste0(item$name, "_cleaned"))
        
        # Save the cleaned data
        saveRDS(
          cleaned_df,
          file = file.path(processed_dir, paste0(item$name, "_cleaned.rds"))
        )
        
        # Store results for reporting
        results[[item$name]] <- list(
          success = TRUE,
          raw_rows = nrow(item$df),
          cleaned_rows = nrow(cleaned_df),
          memory_raw = format(object.size(item$df), units = "Mb"),
          memory_cleaned = format(object.size(cleaned_df), units = "Mb")
        )
        
        # Clean up memory
        rm(cleaned_df)
        gc(verbose = FALSE)
        
        message("Successfully processed ", item$name)
      }, error = function(e) {
        message("ERROR processing ", item$name, ": ", e$message)
        results[[item$name]] <- list(
          success = FALSE,
          error = e$message
        )
      })
    } else {
      message("Skipping ", item$name, " - data is NULL")
      results[[item$name]] <- list(
        success = FALSE,
        error = "Data is NULL"
      )
    }
  }
  
  # Generate summary report
  summary_file <- file.path(here::here("logs"), "cleaning_summary.txt")
  sink(summary_file)
  
  cat("========================================================\n")
  cat("Data Cleaning Summary Report\n")
  cat("========================================================\n")
  cat("Executed at:", format(Sys.time()), "\n\n")
  
  cat("RESULTS BY DATASET:\n")
  for (name in names(results)) {
    cat("\n", name, ":\n")
    cat("  Status: ", ifelse(results[[name]]$success, "SUCCESS", "FAILED"), "\n")
    
    if (results[[name]]$success) {
      cat("  Rows (raw):     ", results[[name]]$raw_rows, "\n")
      cat("  Rows (cleaned): ", results[[name]]$cleaned_rows, "\n")
      cat("  Memory (raw):   ", results[[name]]$memory_raw, "\n")
      cat("  Memory (cleaned):", results[[name]]$memory_cleaned, "\n")
    } else {
      cat("  Error: ", results[[name]]$error, "\n")
    }
  }
  
  cat("\n========================================================\n")
  
  # Calculate execution time
  end_time <- Sys.time()
  execution_time <- difftime(end_time, start_time, units = "mins")
  cat("Total execution time:", format(execution_time), "minutes\n")
  
  sink()
  
  message("\nData cleaning process completed!")
  message("Execution time: ", format(execution_time), " minutes")
  message("Summary saved to: ", summary_file)
  message("Cleaned data saved to: ", processed_dir)
}

# Run main() if this script is executed directly (non-interactive mode)
if (!interactive()) {
  main()
}