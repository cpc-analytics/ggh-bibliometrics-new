# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr)

# Directory where your combined CSVs live
cleaned_dir <- here("data/2-cleaned")

# ─── PubMed ────────────────────────────────────────────────────────────────

# Read PubMed
pubmed_file <- file.path(cleaned_dir, "pubmed_papers_combined_2014_2024.csv")
message(glue("Processing PubMed file: {basename(pubmed_file)}"))

pubmed_df <- read_csv(pubmed_file, show_col_types = FALSE) %>%
  # Trim whitespace on identifiers
  mutate(
    doi    = str_trim(doi),
    pmid   = str_trim(pmid),
    origin = as.character(origin)
  ) %>%
  rename(publication_year = year) %>%
  mutate(
    db_source = "PubMed",
    article_type = tolower(article_type),
    article_type = case_when(
      article_type %in% c("journal article", "introductory journal article") ~ "article",
      article_type %in% c("editorial") ~ "editorial",
      article_type %in% c("letter") ~ "letter",
      article_type %in% c("case reports") ~ "case report",
      article_type %in% c("book", "book chapter", "book-chapter") ~ "book-chapter",
      article_type %in% c("comment") ~ "comment",
      TRUE ~ article_type
    ),
  )

n_before_pb <- nrow(pubmed_df)

pubmed_dedup <- pubmed_df %>%
  # 1) build group_key (DOI → PMID)
  mutate(group_key = coalesce(doi, pmid)) %>%
  
  # 2) branch on NA vs non-NA key
  {
    na_keys   <- filter(., is.na(group_key))         # keep all NA-key rows
    with_keys <- filter(., !is.na(group_key))        # only these get deduped
    
    # 3) dedupe the non-NA groups
    deduped <- with_keys %>%
      group_by(group_key) %>%
      mutate(both_sources = all(c("query", "journal") %in% origin)) %>%
      arrange(desc(origin == "journal")) %>%
      slice(1) %>%
      ungroup() %>%
      mutate(origin = if_else(both_sources, "query & journal", origin)) %>%
      select(-both_sources)
    
    # 4) recombine
    bind_rows(na_keys, deduped)
  } %>%
  select(-group_key)

n_after_pb <- nrow(pubmed_dedup)
message(glue("PubMed: deduplicated {n_before_pb - n_after_pb} rows (from {n_before_pb} to {n_after_pb})."))

# Write out
pubmed_out <- file.path(cleaned_dir, "pubmed_papers_combined_2014_2024_dedup.csv")
write_csv(pubmed_dedup, pubmed_out)
message(glue("Saved PubMed deduped to: {basename(pubmed_out)}"))

# ─── OpenAlex ─────────────────────────────────────────────────────────────

# Read OpenAlex
oa_file <- file.path(cleaned_dir, "openalex_papers_combined_2014_2024.csv")
message(glue("Processing OpenAlex file: {basename(oa_file)}"))

openalex_df <- read_csv(oa_file, show_col_types = FALSE) %>%
  filter(is_retracted != TRUE) %>%
  # strip DOI prefix if present
  mutate(doi = str_trim(str_remove(doi, "^https?://doi\\.org/"))) %>%
  rename(
    article_type = type,
    num_citations_oa = cited_by_count,
    journal_title = source
  )

n_before_oa <- nrow(openalex_df)

openalex_dedup <- openalex_df %>%
  mutate(
    doi    = str_trim(doi),
    pmid   = str_trim(pmid),
    oa_id  = str_trim(oa_id),
    origin = as.character(origin)
  ) %>%
  mutate(group_key = coalesce(doi, pmid, oa_id)) %>%
  # 1) Split out the NA‐key rows so they survive unchanged
  { 
    na_keys     <- filter(., is.na(group_key))
    with_keys   <- filter(., !is.na(group_key))
    
    # 2) Dedupe only the non‐NA keys
    deduped_keys <- with_keys %>%
      group_by(group_key) %>%
      mutate(both_sources = all(c("query", "journal") %in% origin)) %>%
      arrange(desc(origin == "journal")) %>%
      slice(1) %>%
      ungroup() %>%
      mutate(origin = if_else(both_sources, "query & journal", origin)) %>%
      select(-both_sources)
    
    # 3) Recombine
    bind_rows(na_keys, deduped_keys)
  } %>%
  select(-group_key)

n_after_oa <- nrow(openalex_dedup)
message(glue("OpenAlex: deduplicated {n_before_oa - n_after_oa} rows (from {n_before_oa} to {n_after_oa})."))

# Write out
oa_out <- file.path(cleaned_dir, "openalex_papers_combined_2014_2024_dedup.csv")
write_csv(openalex_dedup, oa_out)
message(glue("Saved OpenAlex deduped to: {basename(oa_out)}"))


message("All deduplication complete!")
