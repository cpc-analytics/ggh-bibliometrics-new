# Load necessary libraries
pacman::p_load(httr, jsonlite, dplyr, purrr, readr, here, glue, tidyverse, zip)

# Define the directory containing the OpenAlex CSV files
in_path <- here("data/2-cleaned/openalex")
# ============================
# Combine AUTHORS CSVs
# ============================
authors_out_path <- here("data/2-cleaned/openalex_authors_combined_2014_2024.csv")

# Read journal paper CSVs
journal_files <- list.files(
  path = in_path,
  pattern = "^openalex_authors_journal_\\d{4}\\.csv$",
  full.names = TRUE
)

journal_df <- journal_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "journal")

journal_df <- journal_df %>%
  distinct()

nrow(journal_df) #197973

# Pattern to match both query and journal paper CSVs
query_files <- list.files(
  path = in_path, 
  pattern = "^openalex_authors_\\d{4}\\.csv$", 
  full.names = TRUE
)

query_df <- query_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "query")

query_df <- query_df %>%
  distinct()

nrow(query_df) #412562

# Combine and deduplicate
combined_authors_df <- bind_rows(journal_df, query_df) |> 
  distinct()

nrow(combined_authors_df)#610535

# Save combined authors CSV
write_csv(combined_authors_df, authors_out_path)

# ============================
# Combine PAPERS CSVs
# ============================
papers_out_path <- here("data/2-cleaned/openalex_papers_combined_2014_2024.csv")

# Read journal paper CSVs
journal_files <- list.files(
  path = in_path,
  pattern = "^openalex_papers_journal_\\d{4}\\.csv$",
  full.names = TRUE
)

journal_df <- journal_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "journal")

journal_df <- journal_df %>%
  distinct()

nrow(journal_df) #29844

# Pattern to match both query and journal paper CSVs
query_files <- list.files(
  path = in_path, 
  pattern = "^openalex_papers_\\d{4}\\.csv$", 
  full.names = TRUE
)

query_df <- query_files |> 
  map_dfr(read_csv, show_col_types = FALSE) |> 
  mutate(origin = "query")

query_df <- query_df %>%
  distinct()

nrow(query_df) #81081

# Combine and deduplicate
combined_papers_df <- bind_rows(journal_df, query_df) |> 
  distinct()

nrow(combined_papers_df)#110925

# Save combined authors CSV
write_csv(combined_papers_df, papers_out_path)


