#----------------------------------
# ror_api_utils.R
pacman::p_load(httr, jsonlite, dplyr, purrr, readr, here, glue,
               janitor, stringr, tidyr, writexl, readxl)

# ------------------------------- #
# Global Variables and Constants #
# ------------------------------- #

max_requests <- 2000
time_window <- 300  # 5 minutes
request_times <- c()
max_retries <- 3

# --------------------------------- #
# Rate Limit Handler for ROR API   #
# --------------------------------- #
enforce_rate_limit <- function() {
  current_time <- Sys.time()
  request_times <<- request_times[request_times > (current_time - time_window)]
  
  if (length(request_times) >= max_requests) {
    wait_time <- as.numeric(min(request_times) + time_window - current_time)
    message("API rate limit hit. Sleeping for ", round(wait_time, 2), " seconds...")
    Sys.sleep(wait_time)
  }
  
  request_times <<- c(request_times, current_time)
  Sys.sleep(0.15)
}

# -------------------------------------------------- #
# Helper: Resume from saved results and skip repeats #
# -------------------------------------------------- #
resume_processing <- function(affil_df, output_path, id_col) {
  existing_df <- tibble()
  
  if (file.exists(output_path)) {
    existing_df <- read_xlsx(output_path) %>%
      clean_names()
    
    # Convert 'affiliation' in output to id_col name (e.g., affiliation_cleaned)
    if ("affiliation" %in% names(existing_df)) {
      existing_df <- existing_df %>% rename(!!id_col := affiliation)
    }
    
    if (!(id_col %in% names(existing_df))) {
      stop(glue("Column '{id_col}' not found in saved file. Found: {paste(names(existing_df), collapse = ', ')}"))
    }
    
    existing_df <- existing_df %>%
      mutate("{id_col}" := as.character(.data[[id_col]])) %>%
      distinct()
    
    processed <- existing_df[[id_col]]
    
    affil_df <- affil_df %>%
      mutate("{id_col}" := as.character(.data[[id_col]])) %>%
      filter(!.data[[id_col]] %in% processed)
    
    message("Resuming: ", nrow(affil_df), " remaining out of ", nrow(affil_df) + length(processed))
  }
  
  return(list(remaining = affil_df, existing = existing_df))
}

# ------------------------------------------- #
# Query ROR API for a Single Institution Name #
# ------------------------------------------- #
search_ror <- function(query, id_col = "affiliation") {
  enforce_rate_limit()
  
  empty_row <- tibble(
    affiliation = query,
    ror_id = NA_character_,
    institution = NA_character_,
    score = NA_real_,
    matching_type = NA_character_,
    institution_types = NA_character_,
    country_code = NA_character_,
    country_name = NA_character_,
    lat = NA_character_,
    lng = NA_character_,
    location = NA_character_,
    related_ror_ids = NA_character_,
    related_institutions = NA_character_,
    relationship_types = NA_character_
  )
  
  url <- paste0("https://api.ror.org/v2/organizations?affiliation=", URLencode(query))
  
  for (attempt in seq_len(max_retries)) {
    response <- tryCatch(
      GET(url, timeout(30)),
      error = function(e) {
        message("ROR fetch error (try ", attempt, "): ", e$message)
        return(NULL)
      }
    )
    
    if (is.null(response) || !inherits(response, "response")) {
      Sys.sleep(2^attempt)
      next
    }
    
    if (status_code(response) == 200) break
    Sys.sleep(2^attempt)
  }
  
  if (is.null(response) || status_code(response) != 200) {
    message("Failed after retries: ", query)
    return(empty_row)
  }
  
  content <- fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE)
  if (!"items" %in% names(content) || length(content$items) == 0) {
    message("No match found for: ", query)
    return(empty_row)
  }
  
  best_match <- content$items %>%
    arrange(desc(score)) %>%
    filter(chosen | (!any(chosen) & score == 1)) %>%
    slice(1) %>%
    as_tibble()
  
  if (nrow(best_match) == 0) {
    message("No match found for: ", query)
    return(empty_row)
  }
  
  confidence <- case_when(
    best_match$chosen & best_match$score == 1 ~ "high",
    best_match$score >= 0.80 ~ "medium",
    TRUE ~ "none"
  )
  
  if (confidence == "none") {
    message("Low confidence or no best match for: ", query)
  }
  
  best_match <- best_match %>%
    rename_with(~ str_remove_all(.x, "organization\\.")) %>%
    unnest_wider(locations) %>%
    rename_with(~ str_remove_all(.x, "^geonames_details\\."))
  
  institution_name <- best_match$names %>%
    pluck(1, .default = NA_character_) %>%
    as_tibble() %>%
    unnest(types) %>%
    filter(str_detect(types, "ror_display")) %>%
    pull(value)
  
  out <- tibble(
    !!id_col := query,
    ror_id = best_match$id,
    institution = institution_name,
    score = best_match$score,
    matching_type = best_match$matching_type,
    institution_types = paste(best_match$types[[1]], collapse = "; "),
    country_code = paste(unique(best_match$country_code[[1]]), collapse = "; "),
    country_name = paste(unique(best_match$country_name[[1]]), collapse = "; "),
    lat = paste(unique(best_match$lat[[1]]), collapse = "; "),
    lng = paste(unique(best_match$lng[[1]]), collapse = "; "),
    location = paste(unique(best_match$name[[1]]), collapse = "; ")
  )
  
  if ("relationships" %in% names(best_match)) {
    relationships <- best_match$relationships[[1]]
    if (!is.null(relationships)) {
      rel_df <- as_tibble(relationships)
      out <- out %>%
        mutate(
          related_ror_ids = paste(rel_df$id, collapse = "; "),
          related_institutions = paste(rel_df$label, collapse = "; "),
          relationship_types = paste(rel_df$type, collapse = "; ")
        )
    }
  }
  
  return(out)
}

# ------------------------------------------------- #
# Batch Processor for Affiliations w/ Resume Logic #
# ------------------------------------------------- #
process_affiliations_ror <- function(
  affil_df,
  id_col,
  output_path,
  save_every = 100
) {
  # Resume from existing file if applicable
  resume <- resume_processing(affil_df, output_path, id_col)
  affil_df <- resume$remaining
  existing_df <- resume$existing
  
  if (nrow(affil_df) == 0) {
    message("All affiliations already processed.")
    return(existing_df)
  }
  
  all_results <- existing_df
  for (i in seq_len(nrow(affil_df))) {
    query <- affil_df[[id_col]][i]
    result <- search_ror(query, id_col = id_col)
    all_results <- bind_rows(all_results, result)
    
    if (i %% save_every == 0) {
      write_xlsx(all_results, output_path)
      message("Progress saved at row ", i)
    }
  }
  
  write_xlsx(all_results, output_path)
  message("All affiliations processed and saved.")
  return(all_results)
}


unique_affil <- read_csv("data/ref/cleaned_affil_for_ror.csv")
nrow(unique_affil)#224900

# Assuming unique_affil has column 'revised_clean'
results <- process_affiliations_ror(
  affil_df = unique_affil,
  id_col = "revised_clean",
  output_path = "data/3-merged/ror_affiliations_full_results.xlsx",
  save_every = 50
)

