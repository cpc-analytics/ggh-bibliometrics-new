# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr, purrr, tidyr, tidyverse, data.table)

ror_affiliations_full_results <- read_excel("data/3-merged/ror_affiliations_full_results.xlsx")


pubmed_affil_all <- ror_affiliations_full_results %>%
  left_join(unique_affil, by = "revised_clean") %>%   # Join on 'affiliation' to get 'revised_clean'
  relocate(affiliation, .before = revised_clean) %>%
  full_join(pubmed_author_sep, by = "affiliation") %>%
  relocate(pmid, last_name, first_name, initials, affiliation, .before = revised_clean) %>%
  filter(!is.na(affiliation)) %>%
  distinct()
nrow(pubmed_affil_all)#510545
length(unique(pubmed_affil_all$pmid))#63487


# sanity check------------
pubmed_affil_all %>%
  filter(is.na(affiliation)) %>%
  nrow()


# compare the available affil between pubmed and openalex
openalex_authors <- fread("data/2-cleaned/openalex_authors_combined_2014_2024.csv") %>%
  distinct() %>%
  filter(!is.na(au_affiliation_raw))

nrow(openalex_authors)#523640
length(unique(openalex_authors$pmid))#55696
colnames(openalex_authors)

length(intersect(
  unique(filter(openalex_authors, !is.na(pmid))$pmid),
  unique(filter(pubmed_affil_all, !is.na(pmid))$pmid)
))

pubmed_authors_prepped <- pubmed_affil_all %>%
  mutate(
    au_display_name = str_squish(paste(first_name, last_name))
  ) %>%
  rename(
    au_affiliation_raw = affiliation,
    institution_type = institution_types
  ) %>%
  select(-lat, -lng)
nrow(pubmed_authors_prepped)#510545

openalex_authors_prepped <- openalex_authors %>%
  mutate(
    au_display_name = au_display_name %>%
      str_replace_all("(?<=\\b[A-Z])\\.", "") %>%     # Remove periods after initials
      str_replace_all("(?<=[A-Z])(?=[A-Z])", " ") %>% # Add space between adjacent capitals (e.g., SP → S P)
      str_squish()                                    # Clean up extra whitespace
  )
nrow(openalex_authors_prepped)#523640

author_joined <- full_join(
  pubmed_authors_prepped,
  openalex_authors_prepped,
  by = c("pmid", "au_display_name"),
  suffix = c("_pb", "_oa")
)

author_pubmed_only <- author_joined %>%
  filter(!is.na(au_affiliation_raw_pb)) %>%
  transmute(
    pmid, au_display_name,
    source = "PubMed",
    au_id = NA_character_,
    au_affiliation_raw = au_affiliation_raw_pb,
    institution_id = NA_character_,
    institution_ror = ror_id,
    institution_display_name = institution,
    institution_country_code = country_code,
    institution_type = institution_type_pb
  )

author_openalex_only <- author_joined %>%
  filter(!is.na(au_affiliation_raw_oa)) %>%
  transmute(
    pmid, au_display_name,
    source = "OpenAlex",
    oa_id,
    au_id,
    au_affiliation_raw = au_affiliation_raw_oa,
    institution_id,
    institution_ror,
    institution_display_name,
    institution_country_code,
    institution_type = institution_type_oa
  )

author_combined <- bind_rows(author_pubmed_only, author_openalex_only) %>%
  filter(!is.na(institution_display_name)) %>%
  distinct(pmid, au_display_name, institution_display_name, .keep_all = TRUE) 

pmid_source_summary <- author_combined %>%
  distinct(pmid, source) %>%                # Remove duplicates if same source repeated
  group_by(pmid) %>%
  summarise(source_count = n_distinct(source),
            sources = paste(sort(unique(source)), collapse = " & ")) %>%
  ungroup() %>%
  count(sources, name = "n_pmids")          # Count how many pmids fall into each source pattern

print(pmid_source_summary)

write_csv(author_combined, "data/3-merged/openalex_pubmed_authors_merged_2025-04-30.csv")


low_score_results <- ror_affiliations_full_results %>%
  filter(score == "0.9") %>%
  filter(institution!="Erasmus MC") %>%
  select(revised_clean, ror_id, matching_type, institution)
View(low_score_results)