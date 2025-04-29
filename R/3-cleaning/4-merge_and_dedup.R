# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr, purrr)

# Define directories
cleaned_dir <- here("data/2-cleaned")
merged_dir  <- here("data/3-merged")
dir.create(merged_dir, showWarnings = FALSE, recursive = TRUE)

# --- Read Data ---
pubmed_df   <- read_csv("data/2-cleaned/pubmed_papers_2014_2024_with_citations.csv", show_col_types = FALSE)
openalex_df <- read_csv("data/2-cleaned/openalex_papers_combined_2014_2024_dedup.csv", show_col_types = FALSE)
nrow(pubmed_df)#66264
nrow(openalex_df)#105635

# --- Preprocess ---
pubmed_df <- pubmed_df %>%
  rename(publication_year = year) %>%
  mutate(num_citations_cf = num_citations,
         db_source = "PubMed")

openalex_df <- openalex_df %>%
  mutate(doi = str_remove(doi, "https://doi.org/")) %>% # Clean DOI format
  rename(
    article_type = type,
    num_citations_oa = cited_by_count,
    journal_title = source) %>%
  mutate(
    article_type = case_when(
      article_type == "Journal Article"   ~ "article",
      article_type == "Letter"            ~ "letter",
      article_type == "Editorial"         ~ "editorial",
      article_type == "Review"            ~ "review",
      TRUE ~ article_type),
    article_type = tolower(article_type))


# --- Merge Datasets ---
combined_df <- openalex_df %>%
  full_join(pubmed_df, by = c("pmid", "doi"), suffix = c("_oa", "_pb"))

message(glue("Merged total rows: {nrow(combined_df)}"))

# --- Preprocess before deduplication ---
combined_df_clean <- combined_df %>%
  filter(is_retracted == FALSE) %>%
  mutate(
    from_openalex = db_source_oa == "OpenAlex",  
    from_pubmed   = db_source_pb == "PubMed"     
  ) %>%
  mutate(
    from_openalex = replace_na(from_openalex, FALSE),
    from_pubmed   = replace_na(from_pubmed, FALSE)
  ) %>%
  mutate(
    publication_date = coalesce(publication_date_oa, publication_date_pb),
    title = coalesce(title_pb, title_oa),
    abstract = coalesce(abstract_pb, abstract_oa),
    article_type = coalesce(article_type_oa, article_type_pb),
    journal_title = coalesce(journal_title_oa, journal_title_pb),
    language = coalesce(language_oa, language_pb),
    publication_year = coalesce(publication_year_oa, publication_year_pb)
  ) %>%
  select(-publication_date_oa, -publication_date_pb, -title_pb, -title_oa,
         -abstract_pb, -abstract_oa, -article_type_oa, -article_type_pb,
         -journal_title_oa, -journal_title_pb, -language_oa, -language_pb,
         -publication_year_oa, -publication_year_pb, -is_retracted, -db_source_oa, -db_source_pb)

valid_types <- c(
  "article", "review", "book", "book-chapter", "editorial", "letter",
  "report", "case reports", "meta analysis", "comment", "systematic review", "comparative study"
)

combined_df_clean <- combined_df_clean %>%
  filter(str_to_lower(article_type) %in% valid_types)

message(glue("Cleaned total rows: {nrow(combined_df_clean)}"))

# Check for duplicates
combined_df_clean %>%
  filter(!is.na(doi)) %>%
  count(doi, sort = TRUE) %>%
  filter(n > 1) %>%
  nrow()

combined_df_clean %>%
  filter(!is.na(pmid)) %>%
  count(pmid, sort = TRUE) %>%
  filter(n > 1) %>%
  nrow()

combined_df_clean %>%
  filter(is.na(doi) & is.na(pmid)) %>%
  nrow()

combined_df_clean %>% count(from_openalex, from_pubmed)

# --- Deduplicate After Merge ---
dedup_combined_df <- combined_df_clean %>%
  mutate(
    doi = str_trim(doi),
    pmid = str_trim(pmid),
    oa_id = str_trim(oa_id)
  ) %>%
  arrange(desc(publication_year)) %>%
  
  mutate(grouping_key = case_when(
    !is.na(doi) ~ paste0("doi:", doi),
    is.na(doi) & !is.na(pmid) ~ paste0("pmid:", pmid),
    is.na(doi) & is.na(pmid) & !is.na(oa_id) ~ paste0("oa_id:", oa_id),
    TRUE ~ NA_character_
  )) %>%
  
  group_by(grouping_key) %>%
  filter(row_number() == 1) %>%
  ungroup() %>%
  
  select(-grouping_key)

message(glue("Deduplicated to {nrow(dedup_combined_df)} rows."))

# --- Save Output ---
output_file <- file.path(merged_dir, glue("openalex_pubmed_papers_merged_{Sys.Date()}.csv"))
write_csv(dedup_combined_df, output_file)

message(glue("Saved merged and deduplicated dataset to: {output_file}"))
message("Merge and deduplication process completed.")