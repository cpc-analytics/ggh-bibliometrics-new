# Load necessary libraries
pacman::p_load(httr, jsonlite, dplyr, purrr, readr, here, glue, tidyverse, zip)

# Define the directory containing the PubMed CSV files
in_path <- here("data/2-cleaned/pubmed")

# ============================
# Combine AUTHORS CSVs
# ============================
authors_out_path <- here("data/2-cleaned/pubmed_authors_combined_2014_2024.csv")

# Correct pattern for authors: matches authors_YYYY.csv and journal_authors_YYYY.csv
authors_csv_files <- list.files(
  path = in_path, 
  pattern = "^(authors|journal_authors)_\\d{4}\\.csv$",  
  full.names = TRUE
)

if (length(authors_csv_files) > 0) {
  combined_authors_df <- authors_csv_files |> 
    map_dfr(~ read_csv(.)) %>% 
    distinct()
  
  write_csv(combined_authors_df, authors_out_path)
  print(glue("Combined {length(authors_csv_files)} author files and saved to {authors_out_path}"))
} else {
  print("No author CSV files found.")
}

# ============================
# Combine PAPERS CSVs
# ============================
papers_out_path <- here("data/2-cleaned/pubmed_papers_combined_2014_2024.csv")

# Correct pattern for papers: matches papers_YYYY.csv and journal_papers_YYYY.csv
papers_csv_files <- list.files(
  path = in_path, 
  pattern = "^(papers|journal_papers)_\\d{4}\\.csv$", 
  full.names = TRUE
)

if (length(papers_csv_files) > 0) {
  combined_papers_df <- papers_csv_files |> 
    map_dfr(~ read_csv(.)) %>% 
    distinct()
  
  write_csv(combined_papers_df, papers_out_path)
  print(glue("Combined {length(papers_csv_files)} paper files and saved to {papers_out_path}"))
} else {
  print("No paper CSV files found.")
}
