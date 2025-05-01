# Load required libraries
pacman::p_load(tidyverse, rentrez, XML, xml2, here, glue)

# Load API key from environment variable
api_key <- "4e7696f07a04e00b907d0f55a80aaa100808"
set_entrez_key(api_key)

# Output directory
output_dir <- here("data/1-raw/pubmed_journal")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Helper function to sanitize journal names for filenames
sanitize_filename <- function(name) {
  name %>% 
    str_replace_all("\\s+", "_") %>%      # Replace spaces with underscores
    str_replace_all("[^A-Za-z0-9_]", "") # Remove non-alphanumeric except underscore
}

# Function to fetch articles by journal and year
fetch_articles_by_journal <- function(journal_abbr, year, output_dir, batch_size = 1000, delay = 1, max_retries = 10) {
  message("Fetching data for journal: ", journal_abbr, " | Year: ", year)
  
  query <- paste0(journal_abbr, "[ta] AND ", year, "[pdat]")
  search_results <- entrez_search(db = "pubmed", term = query, use_history = TRUE)
  total_records <- search_results$count
  
  if (total_records == 0) {
    message("No records found for ", journal_abbr, " in ", year)
    return(invisible(NULL))
  }
  
  message("Total records to fetch: ", total_records)
  
  retstart <- 0
  safe_journal_name <- sanitize_filename(journal_abbr)
  
  while (retstart < total_records) {
    retries <- 0
    success <- FALSE
    
    while (!success && retries < max_retries) {
      tryCatch({
        records <- entrez_fetch(
          db = "pubmed",
          web_history = search_results$web_history,
          retmax = batch_size,
          retstart = retstart,
          rettype = "xml",
          parsed = TRUE
        )
        
        batch_end <- min(retstart + batch_size, total_records)
        file_name <- file.path(output_dir, paste0("pubmed_", safe_journal_name, "_", year, "_", retstart + 1, "_to_", batch_end, ".xml"))
        XML::saveXML(records, file_name)
        
        message("Processed batch: ", retstart + 1, " to ", batch_end)
        
        retstart <- retstart + batch_size
        success <- TRUE
      }, error = function(e) {
        retries <<- retries + 1
        message("Error at batch starting ", retstart, ", retry ", retries, ": ", e$message)
        Sys.sleep(delay * 2^retries) # Exponential backoff
      })
    }
    
    if (!success) {
      message("Failed after ", max_retries, " retries for ", journal_abbr, " ", year, " at batch ", retstart)
      retstart <- retstart + batch_size
    } else {
      Sys.sleep(delay)
    }
  }
  
  message("Completed fetching for ", journal_abbr, " in ", year)
}

# List of journal abbreviations
journal_abbreviations <- c(
  "The Lancet Global Health", "Bulletin of the World Health Organization",
  "The Lancet Planetary Health", "BMJ Global Health", "Globalization and Health",
  "Pathogens and Global Health", "Journal of Global Health", "Global Health Research and Policy",
  "Journal of Epidemiology and Global Health", "Cambridge Prisms Global Mental Health",
  "Annals of Global Health", "Global Health Science and Practice", "Global Public Health",
  "Clinical Epidemiology and Global Health", "Global Health Action", "Global Health Promotion",
  "PLOS Global Public Health"
)

# Define years range
years <- 2014:2024

# Loop through journals and years
for (journal in journal_abbreviations) {
  for (year in years) {
    fetch_articles_by_journal(journal, year, output_dir)
  }
}

message("All journal data fetching complete for 2014-2024.")
