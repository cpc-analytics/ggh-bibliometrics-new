# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr, tidyr)

# Define directories
cleaned_dir <- here("data/2-cleaned")
merged_dir  <- here("data/3-merged")
dir.create(merged_dir, showWarnings = FALSE, recursive = TRUE)

# --- Read Data ---
pubmed_df   <- read_csv("data/2-cleaned/pubmed_papers_2014_2024_with_citations.csv", show_col_types = FALSE)
openalex_df <- read_csv("data/2-cleaned/openalex_papers_combined_2014_2024_dedup.csv", show_col_types = FALSE)
nrow(pubmed_df)#66264
nrow(openalex_df)#105716

# --- Preprocess PubMed ---
pubmed_df <- pubmed_df %>%
  mutate(
    num_citations_cf = num_citations,
    join_key = case_when( # build a join key based on DOI, if not then PMID
      !is.na(doi) ~ doi,
      is.na(doi) & !is.na(pmid) ~ paste0("PMID:", pmid),
      TRUE ~ NA_character_
    )
  )

# --- Preprocess OpenAlex ---
openalex_df <- openalex_df %>%
  mutate(
    join_key = case_when(
      !is.na(doi) ~ doi,
      is.na(doi) & !is.na(pmid) ~ paste0("PMID:", pmid),
      TRUE ~ NA_character_
    )
  )

# --- Full join ---
combined_df <- full_join(openalex_df, pubmed_df, by = "join_key", suffix = c("_oa", "_pb")) %>%
  distinct()

message(glue("Merged total rows: {nrow(combined_df)}"))

# --- Clean & coalesce ---
combined_df_clean <- combined_df %>%
  mutate(
    from_openalex = replace_na(db_source_oa == "OpenAlex", FALSE),
    from_pubmed   = replace_na(db_source_pb == "PubMed", FALSE)) %>%
  
  transmute(
    from_openalex,
    from_pubmed,
    publication_date = coalesce(publication_date_oa, publication_date_pb),
    doi              = coalesce(doi_pb, doi_oa),
    pmid             = coalesce(pmid_pb, pmid_oa),
    title            = coalesce(title_pb, title_oa),
    abstract         = coalesce(abstract_pb, abstract_oa),
    article_type     = coalesce(article_type_oa, article_type_pb),
    journal_title    = coalesce(journal_title_oa, journal_title_pb),
    language         = coalesce(language_oa, language_pb),
    publication_year = coalesce(publication_year_oa, publication_year_pb),
    origin           = coalesce(origin_oa, origin_pb),
    oa_id
  ) %>%
  
  mutate(
    grouping_key = case_when(
      !is.na(doi)               ~ paste0("doi:", doi),
      is.na(doi) & !is.na(pmid) ~ paste0("pmid:", pmid),
      is.na(doi) & is.na(pmid) & !is.na(oa_id) ~ paste0("oa_id:", oa_id),
      TRUE                      ~ NA_character_
    )
  ) %>%
  
  group_by(grouping_key) %>%
  mutate(
    origin = if_else(
      n_distinct(origin) > 1,
      "query & journal",
      origin
    )
  ) %>%
  ungroup() %>%
  
  select(-grouping_key)

glue("Merged total: {nrow(combined_df_clean)} rows")

valid_types <- c(
  "article", "review", "book", "book-chapter", "editorial", "letter",
  "report", "case report", "meta-analysis", "comment", "systematic review", "comparative study"
)

combined_df_clean <- combined_df_clean %>%
  filter(str_to_lower(article_type) %in% valid_types)

message(glue("Cleaned total rows: {nrow(combined_df_clean)}"))

# Check for duplicates
combined_df_clean %>%
  filter(!is.na(doi) | !is.na(pmid) | !is.na(oa_id)) %>%
  mutate(grouping_key = case_when(
    !is.na(doi) ~ paste0("doi:", doi),
    is.na(doi) & !is.na(pmid) ~ paste0("pmid:", pmid),
    is.na(doi) & is.na(pmid) & !is.na(oa_id) ~ paste0("oa_id:", oa_id),
    TRUE ~ NA_character_
  )) %>%
  count(grouping_key, sort = TRUE) %>%
  filter(n > 1)

combined_df_clean %>% count(from_openalex, from_pubmed)

janitor::tabyl(combined_df_clean$origin)

# --- Save Output ---
output_file <- file.path(merged_dir, glue("openalex_pubmed_papers_merged_{Sys.Date()}.csv"))
write_csv(combined_df_clean, output_file)

# Then create a ZIP alongside it:
zip_file <- sub("\\.csv$", ".zip", output_file)
# On most systems R’s utils::zip will invoke the system “zip” command
utils::zip(zipfile = zip_file, files = output_file)

message(glue("Saved merged and deduplicated dataset to: {output_file}"))
message(glue("Also created ZIP archive for sharing: {zip_file}"))