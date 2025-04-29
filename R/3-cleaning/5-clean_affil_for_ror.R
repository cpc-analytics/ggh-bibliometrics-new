# Load required libraries
pacman::p_load(dplyr, readr, here, glue, stringr, purrr, tidyr, tidyverse)

# import pubmed author csv
pubmed_author <- read_csv("data/2-cleaned/pubmed_authors_combined_2014_2024.csv")
nrow(pubmed_author)#447064

separated_affil <- pubmed_author %>%
  mutate(original_affiliation = affiliation) %>%
  separate_rows(affiliation, sep = "\\[-AFFIL-SEP-\\]") %>%   # Split into rows
  select(original_affiliation, affiliation) %>%
  filter(!is.na(affiliation)) %>%                                # Remove NA
  mutate(affiliation = str_squish(affiliation)) %>%              # Trim extra spaces
  distinct()                                          

nrow(separated_affil)#310173

unique_affil <- separated_affil %>%
  select(affiliation) %>%
  distinct()
nrow(unique_affil)#254450

# -----clean the strings -------
balanced_clean_affiliation <- function(text_vector) {
  text_vector %>%
    # 1. Remove emails & URLs
    str_remove_all("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b") %>%
    str_remove_all("http[s]?://\\S+") %>%
    
    # 2. Remove 'Electronic address', 'E-mail', 'Fax' mentions
    str_remove_all("(?i)(electronic address:|e-mail:|fax:)") %>%
    
    # 3. Remove leading numbers or single letters
    str_remove_all("^\\s*\\d+[\\.\\s]*") %>%     
    str_remove_all("^\\s*[a-zA-Z]\\s+") %>%      
    str_remove_all("^\\s*\\d+[A-Za-z]*") %>%     
    
    # 4. Remove postal codes (numbers OR alphanumerics like M5G 1X8)
    str_remove_all("\\b[0-9]{4,6}\\b") %>%                 
    str_remove_all("\\b[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}\\b") %>%  
    
    # 5. Remove empty commas or duplicated commas
    str_replace_all(",\\s*,", ", ") %>%        
    str_replace_all(",\\s*$", "") %>%          
    
    # 6. Clean spaces around commas properly
    str_replace_all("\\s*,\\s*", ", ") %>%     
    
    # 7. Remove **any trailing punctuation**
    str_remove_all("[\\p{Punct}\\s]+$") %>%   # Removes trailing punctuation AND spaces after it
    
    # 8. Final cleanup: normalize spaces
    str_squish() %>%
    
    # 9. Remove if string is just punctuation
    str_replace_all("^\\p{Punct}+$", "")  
}

unique_affil <- unique_affil %>%
  mutate(
    revised_clean = balanced_clean_affiliation(affiliation)
  ) %>%
  filter(revised_clean != "") %>%
  filter(!is.na(revised_clean))

separated_affil_cleaned <- separated_affil %>%
  left_join(unique_affil, by = "affiliation")   # Join on 'affiliation' to get 'revised_clean'
nrow(separated_affil_cleaned)#310173

write_csv(separated_affil_cleaned, "data/ref/affil_map_table.csv")

revised_clean_only <- separated_affil_cleaned %>%
  select(revised_clean) %>%
  filter(!is.na(revised_clean)) %>%
  distinct()

revised_clean_only <- revised_clean_only %>%
  filter(revised_clean != "Email")
nrow(revised_clean_only)#224900

write_csv(revised_clean_only, "data/ref/cleaned_affil_for_ror.csv")

