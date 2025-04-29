# ================================
# ✨ QUICK MERGE MODE: UPDATE CITATIONS ✨
# If you ALREADY fetched citation data before,
# and you only want to merge it with a NEW subject matter file,
# run ONLY THIS BLOCK (no need to rerun the full script)
# ================================

# Load required libraries
library(tidyverse)
library(here)

# Paths
output_dir <- here("data/2-cleaned")
output_path <- file.path(output_dir, "pubmed_papers_2014_2024_with_citations.csv")

# Read the latest citations (previously fetched)
citation_results <- read_csv(output_path, show_col_types = FALSE) %>%
  distinct(doi, .keep_all = TRUE)

# === CLEAN MERGED CITATION DATA ===
citation_results_clean <- citation_results %>%
  mutate(
    pmid = coalesce(pmid.x, pmid.y),
    title = coalesce(title.x, title.y),
    abstract = coalesce(abstract.x, abstract.y),
    language = coalesce(language.x, language.y),
    journal_title = coalesce(journal_title.x, journal_title.y),
    article_type = coalesce(article_type.x, article_type.y),
    search_date = coalesce(search_date.x, search_date.y),
    year = coalesce(year.x, year.y)
  ) %>%
  select(
    pmid, num_citations
  )


# Read your updated subject matter dataset
new_subject_matter <- read_csv(here("data/2-cleaned/pubmed_papers_combined_2014_2024_dedup.csv"))

# Start with DOI-based join
updated_dataset <- new_subject_matter %>%
  left_join(citation_results_clean, by = "pmid")

# Save to a new file (so you don't overwrite old ones accidentally)
new_output_path <- file.path(output_dir, "pubmed_papers_2014_2024_with_citations.csv")
write_csv(updated_dataset, new_output_path)

# ================================
# END OF QUICK MERGE MODE
# ================================

# ================================
# 🛠️ FULL FETCH MODE: FETCH CITATIONS FROM CROSSREF
# Only run if you want to actually fetch citations (slow!)
# ================================

# Load required libraries
pacman::p_load(tidyverse, here, httr, jsonlite, furrr, glue)

# Read existing citation data if it exists
output_dir <- here("data/2-cleaned")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_path <- file.path(output_dir, "pubmed_papers_2014_2024_with_citations.csv")

#read_csv(output_path) %>%
#  filter(str_detect(doi, "^10\\.1016")) %>%
#  arrange(desc(num_citations)) %>%
#  head()

# Define batch size (adjust based on API limits)
BATCH_SIZE <- 1000
RATE_LIMIT <- 5  

# Load subject matter dataset
subject_matter <- read_csv(here("data/2-cleaned/pubmed_papers_combined_2014_2024_dedup.csv"))

# Select necessary columns
subject_matter_slim <- subject_matter %>% 
  select(pmid, doi) %>%
  filter(!is.na(doi)) %>%
  mutate(num_citations = NA_integer_)  # Pre-allocate column with NA

# Checkpointing: Load previous results and exclude completed DOIs
if (file.exists(output_path)) {
  completed_citations <- read_csv(output_path, show_col_types = FALSE)
  completed_dois <- completed_citations$doi
  subject_matter_slim <- subject_matter_slim %>% 
    filter(!doi %in% completed_dois)  # Exclude already processed DOIs
  message(glue("Resuming from previous run. Skipping {length(completed_dois)} already processed DOIs."))
} else {
  completed_citations <- NULL
}

remaining_count <- nrow(subject_matter_slim)
total_count <- nrow(subject_matter)
estimated_batches <- ceiling(remaining_count / BATCH_SIZE)

message(glue("{remaining_count} DOIs remaining to process out of {total_count} total."))
message(glue("Estimated {estimated_batches} batches to process (batch size = {BATCH_SIZE})."))

# --- Fetch Function ---
# Function to fetch citation counts for a batch of DOIs
fetch_citation_batch <- function(doi_batch, batch_number, total_batches) {
  print(glue("Processing batch {batch_number} of {total_batches}... ({length(doi_batch)} DOIs)"))
  
  batch_results <- map_df(doi_batch, function(current_doi) {
    current_doi <- str_trim(current_doi)  # Trim just in case
    print(glue("Checking DOI: {current_doi}"))
    
    result <- tryCatch({
      url <- glue("https://api.crossref.org/works/{current_doi}")
      res <- RETRY(
        "GET",
        url,
        timeout(30),
        times = 3,
        pause_min = 2,
        user_agent("YourAppName (mailto:x.wu@cpc-analytics.com)")
      )
      
      if (status_code(res) == 200) {
        citation_df <- content(res, as = "parsed")
        count <- citation_df$message$`is-referenced-by-count`
        if (!is.null(count)) {
          print(glue("Found: {count} citations"))
          return(tibble(
            doi = current_doi,
            num_citations = as.integer(count)
          ))
        } else {
          print(glue("No citation count available"))
        }
      } else if (status_code(res) == 404) {
        print(glue("DOI not found (404)"))
      } else {
        print(glue("Uexpected status: {status_code(res)}"))
      }
      
      return(tibble(doi = current_doi, num_citations = NA_integer_))
      
    }, error = function(e) {
      print(glue("Error fetching DOI {current_doi}: {e$message}"))
      return(tibble(doi = current_doi, num_citations = NA_integer_))
    })
    
    Sys.sleep(1 / RATE_LIMIT)  
    return(result)
  })
  
  print(glue("Finished batch {batch_number}\n"))
  return(batch_results)
}


# Split DOIs into chunks
doi_batches <- split(subject_matter_slim$doi, ceiling(seq_along(subject_matter_slim$doi) / BATCH_SIZE))
total_batches <- length(doi_batches)

# Set up parallel processing
plan(multisession, workers = parallel::detectCores() - 2)

# Fetch citations in batches with checkpointing
for (i in seq_along(doi_batches)) {
  # Refresh completed DOIs before each batch to handle interruptions
  if (file.exists(output_path)) {
    completed_dois <- read_csv(output_path, show_col_types = FALSE) %>% pull(doi)
  } else {
    completed_dois <- character(0)
  }
  
  doi_batch <- doi_batches[[i]]
  doi_batch <- setdiff(doi_batch, completed_dois)  # Remove already processed DOIs
  
  if (length(doi_batch) == 0) {
    print(glue("Batch {i} already completed. Skipping..."))
    next
  }
  
  batch_results <- fetch_citation_batch(doi_batch, i, total_batches)
  
  if (!is.null(batch_results)) {
    write_csv(batch_results, output_path, append = file.exists(output_path))
    print(glue("Saved batch {i}/{total_batches} to {output_path}"))
  }
}


# Merge previously completed citations
if (!is.null(completed_citations)) {
  citation_results <- bind_rows(completed_citations, read_csv(output_path, show_col_types = FALSE))
} else {
  citation_results <- read_csv(output_path, show_col_types = FALSE)
}

citation_results <- citation_results %>%
  distinct(doi, .keep_all = TRUE)

# Merge back into original dataset
updated_citations <- subject_matter %>%
  left_join(citation_results, by = "doi")

# Save the final merged dataset
write_csv(updated_citations, output_path)
message(glue("Final dataset saved to: {output_path}"))

# Cleanup parallel workers
plan(sequential)

print("Citation fetching and processing complete!")


# Double check the NA, fix some that was due to API rate limit
#all_NA_citation$num_citations <- future_map_int(all_NA_citation$doi, fetch_citation_count)

# Merge updated values back into the original dataset
#updated_citations <- subject_matter_citations %>%
#  rows_update(all_NA_citation, by = "doi")  # Update only rows with new data

# Remove DOI column from the updated citations dataset
#updated_citations_clean <- updated_citations %>% select(-doi)

# Merge updated citation data into subject_matter
#subject_matter_with_citations <- subject_matter %>%
#  left_join(updated_citations_clean, by = "pmid")  # Ensure correct join column

#subject_matter_with_citations <- subject_matter_with_citations %>%
#  select(-affiliations, -authors)

#write_csv(subject_matter_with_citations, "data/4-results/subject_matter_with_citations.csv")
#zip(zipfile = "data/4-results/subject_matter_with_citations.zip", files = "data/4-results/subject_matter_with_citations.csv")

