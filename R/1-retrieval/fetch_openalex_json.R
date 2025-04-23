# Load necessary packages
pacman::p_load(
  dplyr, glue, here, httr, jsonlite, lubridate, openalexR, purrr, readr, stringr
)

# Set OpenAlex options
options(openalexR.mailto = "x.wu@cpc-analytics.com")

# Define search query
search_query <- '"global health"'

# Define year range
year_range <- 2014:2024

# Define output directory
json_dir <- here("data/1-raw/openalex")
dir.create(json_dir, recursive = TRUE, showWarnings = FALSE)

# Define Quarters (month and day only)
quarters <- list(
  Q1 = c("01-01", "03-31"),
  Q2 = c("04-01", "06-30"),
  Q3 = c("07-01", "09-30"),
  Q4 = c("10-01", "12-31")
)

# Function to Fetch & Save JSON Immediately
fetch_and_save_json <- function(search_query, year, quarter, from_date, to_date) {
  print(glue("Fetching data for {year} - {quarter} ({from_date} to {to_date})..."))
  
  json_file <- file.path(json_dir, glue("openalex_{year}_{quarter}.json"))
  
  if (file.exists(json_file)) {
    print(glue("JSON for {year} - {quarter} already exists. Skipping..."))
    return(NULL)
  }
  
  retry_count <- 0
  max_retries <- 3
  
  while (retry_count < max_retries) {
    print(glue("Attempt {retry_count + 1} of {max_retries} for {year} - {quarter}..."))
    
    search_results <- tryCatch({
      res <- oa_fetch(
        entity = "works",
        title_and_abstract.search = search_query,
        from_publication_date = from_date,
        to_publication_date = to_date,
        per_page = 200,
        paging = "cursor",
        verbose = TRUE
      )
      res
    }, error = function(e) {
      warning(glue("Error fetching data for {year} - {quarter}: {e$message}"))
      return(NULL)
    })
    
    if (!is.null(search_results) && nrow(search_results) > 0) {
      print(glue("Retrieved {nrow(search_results)} records for {year} - {quarter}."))
      
      write_json(search_results, json_file, pretty = TRUE, auto_unbox = TRUE)
      print(glue("Saved JSON to {json_file}"))
      
      Sys.sleep(2)
      return(json_file)
    } else {
      print(glue("No valid data found for {year} - {quarter}. Retrying..."))
      retry_count <- retry_count + 1
      Sys.sleep(5)
    }
  }
  
  print(glue("Failed to retrieve data for {year} - {quarter} after {max_retries} attempts. Skipping."))
  return(NULL)
}

# Run Fetching Process Across Years and Quarters
json_files <- list()

for (year in year_range) {
  for (quarter in names(quarters)) {
    from_date <- glue("{year}-{quarters[[quarter]][1]}")
    to_date <- glue("{year}-{quarters[[quarter]][2]}")
    
    json_file <- fetch_and_save_json(search_query, year, quarter, from_date, to_date)
    
    if (!is.null(json_file)) {
      json_files <- append(json_files, json_file)
    }
  }
}

print("OpenAlex JSON data retrieval complete.")
print(glue("Total JSON files saved: {length(json_files)}"))
print(glue("JSON Directory: {json_dir}"))
