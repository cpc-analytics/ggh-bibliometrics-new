# Load necessary libraries
pacman::p_load(httr, jsonlite, dplyr, purrr, readr, here, glue, tidyverse, zip)

# Define the directory containing the PubMed CSV files
in_path <- here("data/2-cleaned/pubmed")

# ============================
# Combine AUTHORS CSVs
# ============================
authors_out_path <- here("data/2-cleaned/pubmed_authors_combined_2014_2024.csv")

# --- Identify Author CSVs Separately ---
query_files <- list.files(
  path = in_path,
  pattern = "^authors_\\d{4}\\.csv$",  # e.g., authors_2020.csv
  full.names = TRUE
)

journal_files <- list.files(
  path = in_path,
  pattern = "^journal_authors_\\d{4}\\.csv$",  # e.g., journal_authors_2020.csv
  full.names = TRUE
)

# --- Load and Tag Query Author Data ---
query_df <- map_dfr(query_files, ~ read_csv(.x, show_col_types = FALSE)) %>%
  mutate(origin = "query")

# --- Load and Tag Journal Author Data ---
journal_df <- map_dfr(journal_files, ~ read_csv(.x, show_col_types = FALSE)) %>%
  mutate(origin = "journal")

# --- Combine and Deduplicate ---
combined_authors_df <- bind_rows(query_df, journal_df) %>%
  distinct()

# --- Save Output ---
write_csv(combined_authors_df, authors_out_path)

# ============================
# Combine PAPERS CSVs
# ============================
papers_out_path <- here("data/2-cleaned/pubmed_papers_combined_2014_2024.csv")

# Correct pattern for papers: matches papers_YYYY.csv and journal_papers_YYYY.csv
journal_files <- list.files(
  path = in_path,
  pattern = "^journal_papers_\\d{4}\\.csv$",
  full.names = TRUE
)

journal_df <- journal_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "journal")

journal_df <- journal_df %>%
  distinct()

nrow(journal_df)#24786

query_files <- list.files(
  path = in_path,
  pattern = "^papers_\\d{4}\\.csv$",
  full.names = TRUE
)

query_df <- query_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "query")

query_df <- query_df %>%
  distinct()

nrow(query_df)#44021

# Combine and deduplicate
combined_papers_df <- bind_rows(journal_df, query_df) |> 
  distinct()

nrow(combined_papers_df)#68807

# Save the result
write_csv(combined_papers_df, papers_out_path)

