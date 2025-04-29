# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr)

# Define directory
merged_dir <- here("data/2-cleaned")

# List of files to process
paper_files <- list.files(
  path = merged_dir,
  pattern = "^(openalex|pubmed)_papers_combined_\\d{4}_\\d{4}\\.csv$",
  full.names = TRUE
)

if (length(paper_files) == 0) {
  stop("No combined paper files found in data/2-cleaned")
}

# Process each file
for (file in paper_files) {
  message(glue("Processing file: {basename(file)}"))
  
  papers_df <- read_csv(file, show_col_types = FALSE) %>%
    mutate(
      doi  = str_trim(doi),
      pmid = str_trim(pmid)
    )
  
  before_n <- nrow(papers_df)
  
  # Deduplicate logic
  deduped_df <- papers_df %>%
    # First, remove duplicates where DOI is available
    mutate(doi_flag = if_else(is.na(doi), FALSE, TRUE)) %>%
    group_by(doi_flag) %>%
    filter(!doi_flag | row_number() == 1 | !duplicated(doi)) %>%
    ungroup() %>%
    # Then, remove duplicates where PMIDs exist
    mutate(pmid_flag = if_else(is.na(pmid), FALSE, TRUE)) %>%
    group_by(pmid_flag) %>%
    filter(!pmid_flag | row_number() == 1 | !duplicated(pmid)) %>%
    ungroup() %>%
    select(-doi_flag, -pmid_flag)
  
  after_n <- nrow(deduped_df)
  removed_n <- before_n - after_n
  
  message(glue("Deduplicated {removed_n} rows (from {before_n} to {after_n})."))
  
  # Define output file
  dedup_file <- str_replace(basename(file), "\\.csv$", "_dedup.csv")
  dedup_path <- file.path(merged_dir, dedup_file)
  
  write_csv(deduped_df, dedup_path)
  message(glue("Saved deduplicated file to: {dedup_path}\n"))
}

message("Deduplication process completed.")
