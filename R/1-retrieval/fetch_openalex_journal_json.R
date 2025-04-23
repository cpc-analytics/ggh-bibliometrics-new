# Setup ------------------------------------------------------------------------
pacman::p_load(
  dplyr, ggplot2, glue, here, httr, janitor, jsonlite, lubridate,
  openalexR, purrr, readr, stringr, tidyr, readxl
)

# Set OpenAlex options
options(openalexR.mailto = "x.wu@cpc-analytics.com")

# Get journal IDs from OpenAlex-------------------------------------------------
#issn <- "2767-3375"
#url <- paste0("https://api.openalex.org/sources/issn:", issn)

#response <- GET(url)
#data <- fromJSON(content(response,s "text", encoding = "UTF-8"))

# View the OpenAlex source ID
#data$id

# View the journal name
#data$display_name

# The list of journals as well as it's issn and openalex journal id
journal_issn_id <- read_excel("data/ref/openalex_journal_issn_id.xlsx")

# List of OpenAlex source IDs (journal IDs)
journal_ids <- journal_issn_id$journal_id

# Year range
year_seq <- as.character(2014:2024)

# Output folder for JSON
json_dir <- here("data/1-raw/openalex_journal")
dir.create(json_dir, recursive = TRUE, showWarnings = FALSE)

api_calls_made <- 0
api_call_limit <- 100000

# Function to fetch and save OpenAlex data by journal and year (JSON only)
fetch_journal_year <- function(jid, year) {
  message(glue("Fetching for Journal ID {jid} — Year {year}"))
  
  json_path <- file.path(json_dir, glue("openalex_{jid}_{year}.json"))
  
  if (file.exists(json_path)) {
    message(glue("JSON already exists for {jid} {year}, skipping."))
    return(json_path)
  }
  
  # Attempt fetch
  result <- tryCatch({
    oa_fetch(
      entity = "works",
      locations.source.id = jid,
      publication_year = year,
      per_page = 200,
      paging = "cursor",
      verbose = TRUE
    )
  }, error = function(e) {
    warning(glue("Error fetching data for {jid} in {year}: {e$message}"))
    return(NULL)
  })
  
  if (is.null(result) || nrow(result) == 0) {
    message(glue("No data retrieved for {jid} in {year}"))
    return(NULL)
  }
  
  # Save raw JSON
  write_json(result, json_path, pretty = TRUE, auto_unbox = TRUE)
  Sys.sleep(1)
  return(json_path)
}

# Fetch data for all journals and years
fetched_paths <- list()

for (year in year_seq) {
  for (jid in journal_ids) {
    file <- fetch_journal_year(jid, year)
    if (!is.null(file)) {
      fetched_paths <- append(fetched_paths, file)
    }
    
    api_calls_made <- api_calls_made + 1
    if (api_calls_made >= api_call_limit) {
      warning("API call limit reached.")
      break
    }
  }
}

message("All available journal-year JSON files fetched!")
message(glue("Total JSON files saved: {length(fetched_paths)}"))
