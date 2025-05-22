# Load necessary packages
pacman::p_load(
  dplyr, glue, here, jsonlite, openalexR, readr, stringr, purrr, janitor, tidyr
)

# Set OpenAlex contact email
options(openalexR.mailto = "x.wu@cpc-analytics.com")

# Define search query
search_query <- '("uncertainty" OR "uncertainties") 
                 NOT ("uncertainty estimation" OR "uncertainty interval" OR "uncertainty intervals" 
                      OR "uncertainty ranges" OR "modeling uncertainty" OR "uncertainty quantification" 
                      OR "uncertainty visualization" OR "uncertainty relation" 
                      OR "sensor" OR "sensors")'

# Define filters
filters <- list(
  type = "types/article",
  primary_topic.domain.id = c("!domains/3", "!domains/1"),   # Exclude Physical & Life Sciences
  primary_topic.field.id = "fields/33"                       # Include Social Sciences
)

# Define output directory and CSV name
out_dir <- here("data/1-raw/openalex")
if (!file.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

output_csv <- glue("{out_dir}/openalex_uncertainty_social_sciences.csv")

# API settings
api_calls_made <- 0
api_call_limit <- 100000
per_page <- 200

# Pre-check total number of records
total_records <- oa_fetch(
  entity = "works",
  title_and_abstract.search = search_query,
  type = filters$type,
  primary_topic.domain.id = filters$primary_topic.domain.id,
  primary_topic.field.id = filters$primary_topic.field.id,
  count_only = TRUE
)[[1]]

estimated_pages <- ceiling(total_records / per_page)

print(glue("Total records matching query: {format(total_records, big.mark = ',')}"))
print(glue("Estimated pages to process: {estimated_pages}"))

if (estimated_pages + api_calls_made > api_call_limit) {
  stop("API call limit will be exceeded. Consider splitting the query or running on another day.")
}

# Start fetching
print("Starting OpenAlex data fetch...")

search_results <- tryCatch({
  res <- oa_fetch(
    entity = "works",
    title_and_abstract.search = search_query,
    type = filters$type,
    primary_topic.domain.id = filters$primary_topic.domain.id,
    primary_topic.field.id = filters$primary_topic.field.id,
    per_page = per_page,
    paging = "cursor",
    verbose = TRUE
  )
  
  assign("api_calls_made", api_calls_made + estimated_pages, envir = .GlobalEnv)
  res
}, error = function(e) {
  warning(glue("Error fetching data: {e$message}"))
  return(NULL)
})

if (is.null(search_results) || nrow(search_results) == 0) {
  print("No results fetched. Please review your query and filters.")
} else {
  print(glue("Total records retrieved: {nrow(search_results)}"))
  print(glue("Total API calls made: {api_calls_made}"))
  
  # Process PubMed IDs
  pmid_df <- search_results %>%
    mutate(pmid = map_chr(ids, ~ .x["pmid"] %||% NA_character_)) %>%
    select(id, pmid) %>%
    filter(!is.na(pmid)) %>%
    distinct()
  
  # Process author information
  author_df <- search_results %>%
    unnest(cols = c(author)) %>%
    group_by(id) %>%
    reframe(
      id,
      au_id = paste(unique(au_id), collapse = " | "),
      au_display_name = paste(unique(au_display_name), collapse = " | "),
      au_affiliation_raw = paste(unique(au_affiliation_raw), collapse = " | "),
      institution_id = paste(unique(institution_id), collapse = " | "),
      institution_display_name = paste(unique(institution_display_name), collapse = " | "),
      institution_country_code = paste(unique(institution_country_code), collapse = " | "),
      institution_type = paste(unique(institution_type), collapse = " | ")
    ) %>%
    distinct()
  
  # Process funding information
  grants_df <- search_results %>%
    filter(!is.na(grants)) %>%
    mutate(
      funder = map(grants, ~ .x[names(.x) == "funder"]),
      funder_display_name = map(grants, ~ .x[names(.x) == "funder_display_name"])
    ) %>%
    mutate(
      funder = map_chr(funder, ~ paste(unique(.x), collapse = " | ")),
      funder_display_name = map_chr(funder_display_name, ~ paste(unique(.x), collapse = " | "))
    ) %>%
    select(id, funder, funder_display_name)
  
  # Final data processing
  oa_df <- search_results %>%
    select(-ids, -author, -grants) %>%
    left_join(pmid_df, by = join_by(id)) %>%
    left_join(author_df, by = join_by(id)) %>%
    left_join(grants_df, by = join_by(id)) %>%
    select(
      doi,
      oa_id = id,
      pmid, 
      issn_l,
      title,
      abstract = ab,
      au_id,
      au_display_name,
      au_affiliation_raw,
      institution_id,
      institution_display_name,
      institution_country_code,
      institution_type,
      publication_year,
      publication_date,
      type,
      url,
      pdf_url,
      oa_url,
      version,
      language,
      funder,
      funder_display_name,
      cited_by_count,
      is_retracted,
      source = so
    ) %>%
    mutate(
      publication_date = as.Date(publication_date),
      publication_year = as.integer(publication_year),
      db_source = 'OpenAlex'
    )
  
  # Save to CSV
  write_csv(oa_df, output_csv)
  print(glue("Data successfully saved to: {output_csv}"))
}

print("OpenAlex data retrieval complete.")