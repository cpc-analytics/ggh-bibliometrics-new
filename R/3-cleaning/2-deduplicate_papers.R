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

if (length(paper_files) == 0) stop("No combined paper files found in data/2-cleaned")

for (file in paper_files) {
  message(glue("Processing file: {basename(file)}"))
  
  papers_df <- read_csv(file, show_col_types = FALSE) %>%
    mutate(
      doi    = str_trim(doi),
      pmid   = str_trim(pmid),
      origin = as.character(origin)
    )
  
  before_n <- nrow(papers_df)
  
  deduped_df <- papers_df %>%
    # define the grouping key: DOI if present, otherwise PMID
    mutate(group_key = coalesce(doi, pmid)) %>%
    filter(!is.na(group_key)) %>%
    
    # within each group, mark if both sources exist
    group_by(group_key) %>%
    mutate(
      both_sources = all(c("query","journal") %in% origin)
    ) %>%
    # prefer the journal row first
    arrange(desc(origin == "journal")) %>%
    # pick the top row per group
    slice(1) %>%
    ungroup() %>%
    
    # if both were present, override origin
    mutate(
      origin = if_else(both_sources, "query & journal", origin)
    ) %>%
    select(-group_key, -both_sources)
  
  after_n  <- nrow(deduped_df)
  removed_n <- before_n - after_n
  message(glue("Deduplicated {removed_n} rows (from {before_n} to {after_n})."))
  
  dedup_file <- str_replace(basename(file), "\\.csv$", "_dedup.csv")
  write_csv(deduped_df, file.path(merged_dir, dedup_file))
  message(glue("Saved to: {dedup_file}\n"))
}

message("Deduplication completed.")
