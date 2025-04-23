# Load required packages
pacman::p_load(
  dplyr, tidyr, jsonlite, purrr, readr, stringr, glue, here
)

# PARAMETERS -------------------------------------------------------------------
data_type <- "query"   # <-- CHANGE THIS to "query" or "journal" as needed

if (data_type == "query") {
  json_dir <- here("data/1-raw/openalex")
  csv_prefix_papers  <- "openalex_papers"
  csv_prefix_authors <- "openalex_authors"
  
  year_pattern <- "openalex_\\d{4}_Q\\d\\.json"
  file_pattern <- function(year) glue("openalex_{year}_Q\\d\\.json")
  
} else if (data_type == "journal") {
  json_dir <- here("data/1-raw/openalex_journal")
  csv_prefix_papers  <- "openalex_papers_journal"
  csv_prefix_authors <- "openalex_authors_journal"
  
  year_pattern <- "openalex_.*_\\d{4}\\.json"
  file_pattern <- function(year) glue("openalex_.*_{year}\\.json")
  
} else {
  stop("Invalid data_type specified. Use 'query' or 'journal'.")
}

csv_dir  <- here("data/2-cleaned/openalex")
dir.create(csv_dir, recursive = TRUE, showWarnings = FALSE)

# Function to Parse Single JSON File -------------------------------------------
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
      "publication_year", "publication_date", "type", "url", "oa_url",
      "version", "language", "cited_by_count", "is_retracted", "source" = "so"
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

# --- Process JSON Files by Year -----------------------------------------------
# Dynamic year extraction based on data_type
if (data_type == "query") {
  years <- unique(str_extract(list.files(json_dir, pattern = year_pattern), "(?<=openalex_)\\d{4}(?=_Q\\d\\.json)"))
} else if (data_type == "journal") {
  years <- unique(str_extract(list.files(json_dir, pattern = year_pattern), "(?<=_)\\d{4}(?=\\.json)"))
}

for (year in years) {
  print(glue("Processing year: {year}"))
  
  year_json_files <- list.files(json_dir, pattern = file_pattern(year), full.names = TRUE)
  
  if (length(year_json_files) == 0) {
    warning(glue("No JSON files found for year {year}. Skipping..."))
    next
  }
  
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
  
  if (length(all_papers) == 0) {
    warning(glue("No paper data parsed for {year}. Skipping CSV save."))
    next
  }
  
  # Combine data
  year_papers_df  <- bind_rows(all_papers) |> distinct()
  year_authors_df <- bind_rows(all_authors) |> distinct()
  
  # Define output file paths
  papers_csv  <- file.path(csv_dir, glue("{csv_prefix_papers}_{year}.csv"))
  authors_csv <- file.path(csv_dir, glue("{csv_prefix_authors}_{year}.csv"))
  
  # Save combined CSVs
  write_csv(year_papers_df, papers_csv)
  write_csv(year_authors_df, authors_csv)
  
  print(glue("Saved papers CSV for {year}: {papers_csv}"))
  print(glue("Saved authors CSV for {year}: {authors_csv}"))
}

print("OpenAlex JSON parsing and combining complete.")
