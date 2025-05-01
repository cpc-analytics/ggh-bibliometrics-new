# Load required libraries
pacman::p_load(tidyverse, rentrez, XML, here)

# Set API key (outside function)
api_key <- "4e7696f07a04e00b907d0f55a80aaa100808"
set_entrez_key(api_key)

# Define parameters (outside function)
output_dir <- here("data/1-raw/pubmed")
start_year <- 2014
end_year   <- 2024
query_terms <- '("global health"[Title/Abstract])'

# Ensure output directory exists
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# FUNCTION: Format the date range for a specific year
format_year_for_query <- function(year) {
  paste0('("', year, '/01/01"[Date - Publication] : "', year, '/12/31"[Date - Publication])')
}

# FUNCTION: Fetch and save PubMed metadata
download_pubmed_metadata <- function(year, query_terms, output_dir, batch_size = 1000, delay = 0.2) {
  message("Fetching data for the year: ", year)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  query_date_range <- format_year_for_query(year)
  query <- paste0(query_terms, ' AND ', query_date_range)
  
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
      batch_end <- min(retstart + batch_size, total_records)
      records <- entrez_fetch(
        db = "pubmed",
        web_history = query_search$web_history,
        retmax = batch_size,
        retstart = retstart,
        rettype = "xml",
        parsed = TRUE
      )
      file_name <- file.path(output_dir, paste0("query_results_", year, "_batch_", retstart + 1, "_to_", batch_end, ".xml"))
      XML::saveXML(records, file_name)
      
      message("Processed batch: ", retstart + 1, " to ", batch_end)
      retstart <- retstart + batch_size
      Sys.sleep(delay)
      
    }, error = function(e) {
      message("Error processing batch starting at ", retstart, ": ", e$message)
    })
  }
  message("All batches for ", year, " fetched and saved successfully.")
}

# Update the multi-year function
download_pubmed_data_for_years <- function(start_year, end_year, query_terms, output_dir, ...) {
  for (year in seq(start_year, end_year)) {
    download_pubmed_metadata(year, query_terms, output_dir = output_dir, ...)
  }
}

# Call it like this:
download_pubmed_data_for_years(start_year, end_year, query_terms, output_dir)

message("PubMed data fetching complete.")