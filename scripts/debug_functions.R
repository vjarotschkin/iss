check_integrity_debug <- function(df, df_name, expected_cols = NULL) {
  cat("\n==================================================================\n")
  cat("DETAILED DEBUG: Integrity check for:", df_name, "\n")
  cat("------------------------------------------------------------------\n")
  
  # Check if data exists
  if (is.null(df)) {
    cat("❌ ERROR: Data is NULL\n")
    return(invisible(NULL))
  }
  
  # Basic information
  cat("✓ Data exists\n")
  cat("Dimensions:", paste(dim(df), collapse = " x "), "\n")
  cat("Class:", class(df), "\n")
  
  # Column names
  cat("\nColumns present:", paste(names(df)[1:min(5, length(names(df)))], collapse = ", "), "...\n")
  
  # Sample data (first few rows)
  cat("\nFirst few rows of data:\n")
  print(head(df, 2))
  
  # Check for empty or all-NA columns
  empty_cols <- names(df)[sapply(df, function(x) all(is.na(x) | x == ""))]
  if (length(empty_cols) > 0) {
    cat("\n⚠️ WARNING: Found empty or all-NA columns:", paste(empty_cols, collapse = ", "), "\n")
  }
  
  # Expected columns check
  if (!is.null(expected_cols)) {
    missing_cols <- setdiff(expected_cols, names(df))
    if (length(missing_cols) > 0) {
      cat("\n⚠️ WARNING: Missing expected columns:", paste(missing_cols, collapse = ", "), "\n")
    } else {
      cat("\n✓ All expected columns present\n")
    }
  }
  
  cat("\n==================================================================\n")
  return(invisible(NULL))
}

debug_save_rds <- function(data, filepath) {
  tryCatch({
    if (is.null(data)) {
      cat("❌ Not saving", basename(filepath), "- data is NULL\n")
      return(FALSE)
    }
    
    dir.create(dirname(filepath), showWarnings = FALSE, recursive = TRUE)
    saveRDS(data, file = filepath)
    
    # Verify the save worked
    if (file.exists(filepath)) {
      cat("✓ Successfully saved:", basename(filepath), "\n")
      cat("  File size:", file.size(filepath), "bytes\n")
      return(TRUE)
    } else {
      cat("❌ Failed to save:", basename(filepath), "\n")
      return(FALSE)
    }
  }, error = function(e) {
    cat("❌ Error saving", basename(filepath), ":", e$message, "\n")
    return(FALSE)
  })
}