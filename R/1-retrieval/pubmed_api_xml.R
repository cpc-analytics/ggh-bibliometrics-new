# Load required libraries
library(tidyverse)
library(rentrez)
library(XML)

# Load API key from environment variable
api_key <- Sys.getenv("ENTREZ_API_KEY")
if (api_key == "") {
  stop("API key not found. Please set ENTREZ_API_KEY in your environment variables.")
}
set_entrez_key(api_key)

# Define output filepath
output_dir <- "data/1-raw/pubmed"

# Define query terms for "global health" related terms
query_terms <- '("global health"[Title/Abstract])'

# FUNCTION: Format the date range for a specific year
format_year_for_query <- function(year) {
  paste0('(\"', year, '/01/01\"[Date - Publication] : \"', year, '/12/31\"[Date - Publication])')
}

# FUNCTION: Fetch and save PubMed metadata
download_pubmed_metadata <- function(year, query_terms, output_dir = "data/1-raw/pubmed", batch_size = 1000, delay = 0.2) {
  message("Fetching data for the year: ", year)
  
  # Ensure output directory exists
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  query_date_range <- format_year_for_query(year)
  query <- paste0(query_terms, ' AND ', query_date_range)
  
  # Initial search to get total record count
  query_search <- entrez_search(db = "pubmed", term = query, use_history = TRUE)
  total_records <- query_search$count
  
  if (total_records == 0) {
    message("No records found for year ", year)
    return(invisible(NULL))
  }
  
  message("Total records to fetch: ", total_records)
  
  retstart <- 0
  
  while (retstart < total_records) {
    tryCatch({
      # Fetch records in batches
      records <- entrez_fetch(
        db = "pubmed",
        web_history = query_search$web_history,
        retmax = batch_size,
        retstart = retstart,
        rettype = "xml",
        parsed = TRUE
      )
      
      # Save XML to file
      file_name <- file.path(output_dir, paste0("query_results_", year, "_batch_", retstart + 1, "_to_", retstart + batch_size, ".xml"))
      XML::saveXML(records, file_name)
      
      message("Processed batch: ", retstart + 1, " to ", retstart + batch_size)
      
      retstart <- retstart + batch_size
      
      # Pause to prevent overwhelming the API
      Sys.sleep(delay)
      
    }, error = function(e) {
      message("Error processing batch starting at ", retstart, ": ", e$message)
    })
  }
  
  message("All batches for ", year, " fetched and saved successfully.")
}

# Function to process multiple years
download_pubmed_data_for_years <- function(start_year, end_year, query_terms, ...) {
  for (year in seq(start_year, end_year)) {
    download_pubmed_metadata(year, query_terms, ...)
  }
}

# Define years and execute the function
download_pubmed_data_for_years(2014, 2024, query_terms)
