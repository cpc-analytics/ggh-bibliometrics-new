# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr, purrr, tidyr, tidyverse, data.table, readxl)

# Merge ror results back in the affiliations
ror_affiliations_full_results <- read_excel("data/3-merged/ror_affiliations_full_results.xlsx")
unique_affil <- read_csv("data/2-cleaned/unique_affil.csv")
pubmed_author_sep <- read_csv("data/ref/pubmed_author_sep.csv")

nrow(unique_affil)#254450
length(unique(unique_affil$revised_clean))#224902
nrow(ror_affiliations_full_results)#224900
sum(is.na(ror_affiliations_full_results$institution))#57990

pubmed_affil_all <- ror_affiliations_full_results %>%
  left_join(unique_affil, by = "revised_clean") %>%   # Join on 'affiliation' to get 'revised_clean'
  relocate(affiliation, .before = revised_clean) %>%
  full_join(pubmed_author_sep, by = "affiliation") %>%
  relocate(pmid, last_name, first_name, initials, affiliation, .before = revised_clean) %>%
  distinct() %>%
  mutate(source = "PubMed")

nrow(pubmed_affil_all)#546539
sum(is.na(pubmed_affil_all$institution))#142980
length(unique(pubmed_affil_all$pmid))#65365

# compare the available affil between pubmed and openalex
openalex_authors <- fread("data/2-cleaned/openalex_authors_combined_2014_2024.csv") %>%
  distinct() %>%
  mutate(source = "OpenAlex")

nrow(openalex_authors)#585073
length(unique(openalex_authors$pmid))#57323

length(intersect(
  unique(filter(openalex_authors, !is.na(pmid))$pmid),
  unique(filter(pubmed_affil_all, !is.na(pmid))$pmid)
))
#52386

# Normalize the author name for matching
pubmed_authors_prepped <- pubmed_affil_all %>%
  mutate(
    au_display_name = str_squish(paste(first_name, last_name))
  ) %>%
  rename(
    au_affiliation_raw = affiliation,
    institution_type = institution_types
  ) %>%
  select(-lat, -lng)
nrow(pubmed_authors_prepped)#546539

openalex_authors_prepped <- openalex_authors %>%
  mutate(
    au_display_name = au_display_name %>%
      str_replace_all("(?<=\\b[A-Z])\\.", "") %>%     # Remove periods after initials
      str_replace_all("(?<=[A-Z])(?=[A-Z])", " ") %>% # Add space between adjacent capitals (e.g., SP → S P)
      str_squish()                                    # Clean up extra whitespace
  )
nrow(openalex_authors_prepped)#610535

# full join on pmid, author name
author_joined <- full_join(
  pubmed_authors_prepped,
  openalex_authors_prepped,
  by = c("pmid", "au_display_name"),
  suffix = c("_pb", "_oa")
)

sum(is.na(pubmed_authors_prepped$pmid))#0
sum(is.na(openalex_authors_prepped$pmid))#202622
nrow(author_joined)#893873

# clean up the source column
author_joined <- author_joined %>%
  mutate(
    source = case_when(
      !is.na(source_pb) & !is.na(source_oa) ~ "PubMed & OpenAlex",
      is.na(source_pb) & !is.na(source_oa)  ~ source_oa,
      !is.na(source_pb) & is.na(source_oa)  ~ source_pb,
      TRUE                                  ~ NA_character_
    )
  ) %>%
  select(-source_pb, -source_oa)

# clean up the query/journal origin column
author_joined <- author_joined %>%
  mutate(
    origin = case_when(
      # Both present and equal → just use either value
      !is.na(origin_pb) & !is.na(origin_oa) & origin_pb == origin_oa ~ origin_pb,
      # Both present and different → query & journal
      !is.na(origin_pb) & !is.na(origin_oa) & origin_pb != origin_oa ~ "query & journal",
      # Only PubMed side present
      !is.na(origin_pb) &  is.na(origin_oa)                      ~ origin_pb,
      # Only OpenAlex side present
      is.na(origin_pb)  & !is.na(origin_oa)                     ~ origin_oa,
      # Neither → NA
      TRUE                                                          ~ NA_character_
    )
  ) %>%
  select(-origin_pb, -origin_oa)

author_joined_dedup <- author_joined %>%
  group_by(pmid, au_display_name, au_id) %>%
  summarise(
    # Coalesce columns, prioritize openalex values
    au_affiliation_raw = coalesce(au_affiliation_raw_oa, au_affiliation_raw_pb),
    institution_ror = coalesce(institution_ror, ror_id),
    institution_type   = coalesce(institution_type_oa, institution_type_pb),
    institution_display_name = coalesce(institution_display_name, institution),
    institution_country_code = coalesce(institution_country_code, country_code),
    .groups = "drop"
  ) %>%
  select(
    pmid, oa_id, au_id, au_display_name, au_affiliation_raw,
    institution_ror, institution_display_name,
    institution_country_code, institution_type,
    origin, source, score, related_institutions, 
    relationship_types, related_ror_ids
  ) %>%
  distinct()

nrow(author_joined)
nrow(author_joined_dedup)

author_joined_dedup <- author_joined_dedup %>%
  mutate(primary_id = coalesce(as.character(pmid), as.character(oa_id)))

write_csv(author_joined_dedup, "data/3-merged/openalex_pubmed_authors_merged_{Sys.Date()}.csv")

