# Load necessary libraries
pacman::p_load(httr, jsonlite, dplyr, purrr, readr, here, glue, tidyverse, zip)

# Define the directory containing the OpenAlex CSV files
in_path <- here("data/2-cleaned/openalex")
# ============================
# Combine AUTHORS CSVs
# ============================
authors_out_path <- here("data/3-merged/openalex_authors_combined_2014_2024.csv")

# Get a list of all CSV files in the folder
authors_csv_files <- list.files(
  path = in_path, 
  pattern = "^openalex_authors(_journal)?_\\d{4}\\.csv$",  # Matches both types
  full.names = TRUE
)

# Read and combine all the CSV files into a single data frame
combined_authors_df <- authors_csv_files |> 
  map_dfr(~ read_csv(.)) %>% 
  distinct()

# Save results
write_csv(combined_authors_df, authors_out_path)
print(glue("Combined {length(authors_csv_files)} files and saved to {authors_out_path}"))

# ============================
# Combine PAPERS CSVs
# ============================
papers_out_path <- here("data/3-merged/openalex_papers_combined_2014_2024.csv")

# Pattern to match both query and journal paper CSVs
papers_csv_files <- list.files(
  path = in_path, 
  pattern = "^openalex_papers(_journal)?_\\d{4}\\.csv$", 
  full.names = TRUE
)

# Combine and deduplicate papers data
combined_papers_df <- papers_csv_files |> 
  map_dfr(~ read_csv(.)) %>% 
  distinct()

# Save combined authors CSV
write_csv(combined_papers_df, papers_out_path)
print(glue("Combined {length(papers_csv_files)} files and saved to {papers_out_path}"))


