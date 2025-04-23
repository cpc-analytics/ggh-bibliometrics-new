# Load required packages
pacman::p_load(
  dplyr, tidyr, jsonlite, purrr, readr, stringr, glue, here
)

# Define Input & Output Directories
json_dir <- here("data/1-raw/openalex/json")     # Directory with JSON files
csv_dir  <- here("data/2-cleaned/openalex")      # Directory to save combined CSVs

# Ensure output directory exists
dir.create(csv_dir, recursive = TRUE, showWarnings = FALSE)

# Function to Parse Single JSON File
parse_openalex_json <- function(json_file) {
  print(glue("Parsing JSON: {json_file}..."))
  
  json_data <- fromJSON(json_file, flatten = TRUE)
  
  if (is.null(json_data) || nrow(json_data) == 0) {
    warning(glue("No valid data in {json_file}, skipping."))
    return(list(papers = NULL, authors = NULL))
  }
  
  # Extract pmid safely
  json_data <- json_data |> 
    mutate(pmid = map_chr(ids, function(x) {
      if (is.null(x) || length(x) == 0) return(NA_character_)
      x <- unlist(x)  
      pmid_match <- str_extract(x, "(?<=pubmed\\.ncbi\\.nlm\\.nih\\.gov/)\\d+")
      pmid_match <- na.omit(pmid_match)
      if (length(pmid_match) > 0) return(pmid_match[1])
      return(NA_character_)
    })) |> 
    mutate(pmid = as.character(pmid))
  
  # Extract Paper Data
  papers_df <- json_data |>
    select(-ids, -author) |>  
    select(any_of(c(
      "doi", "oa_id" = "id", "pmid", "issn_l", "title", "abstract" = "ab",
      "publication_year", "publication_date", "type", "url", "pdf_url",
      "oa_url", "version", "language", "cited_by_count", "is_retracted", "source" = "so"
    ))) |> 
    mutate(
      publication_date = as.Date(publication_date),
      publication_year = as.integer(publication_year),
      db_source = 'OpenAlex'
    )
  
  # Extract Author Data
  authors_df <- map2_dfr(json_data$id, json_data$author, function(oa_id, authors) {
    if (is.null(authors) || nrow(authors) == 0) return(NULL)
    
    authors %>%
      mutate(oa_id = oa_id) %>%
      select(any_of(c(  
        "oa_id", "au_id", "au_display_name", "au_affiliation_raw",
        "institution_id", "institution_ror", "institution_display_name",
        "institution_country_code", "institution_type"
      )))
  }) |> 
    left_join(select(papers_df, oa_id, pmid), by = "oa_id")
  
  return(list(papers = papers_df, authors = authors_df))
}

# --- Process JSON Files by Year ---
years <- unique(str_extract(list.files(json_dir, pattern = "openalex_\\d{4}_Q\\d\\.json"), "\\d{4}"))

for (year in years) {
  print(glue("Processing year: {year}"))
  
  year_json_files <- list.files(json_dir, pattern = glue("openalex_{year}_Q\\d\\.json"), full.names = TRUE)
  
  all_papers <- list()
  all_authors <- list()
  
  for (json_file in year_json_files) {
    parsed_data <- parse_openalex_json(json_file)
    
    if (!is.null(parsed_data$papers)) {
      all_papers <- append(all_papers, list(parsed_data$papers))
    }
    if (!is.null(parsed_data$authors)) {
      all_authors <- append(all_authors, list(parsed_data$authors))
    }
  }
  
  # Combine quarterly data
  year_papers_df  <- bind_rows(all_papers) |> distinct()
  year_authors_df <- bind_rows(all_authors) |> distinct()
  
  # Define output file paths
  papers_csv  <- file.path(csv_dir, glue("openalex_papers_{year}.csv"))
  authors_csv <- file.path(csv_dir, glue("openalex_authors_{year}.csv"))
  
  # Save combined CSVs
  write_csv(year_papers_df, papers_csv)
  write_csv(year_authors_df, authors_csv)
  
  print(glue("Saved papers CSV for {year}: {papers_csv}"))
  print(glue("Saved authors CSV for {year}: {authors_csv}"))
}

print("OpenAlex JSON parsing and combining complete.")
