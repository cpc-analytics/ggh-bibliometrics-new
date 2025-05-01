# Load required libraries
pacman::p_load(tidyverse, rentrez, xml2, here, glue)


# PARAMETERS -------------------------------------------------------------------
data_type <- "journal"   # <-- CHANGE THIS to "query" or "journal"

if (data_type == "query") {
  input_dir  <- here("data/1-raw/pubmed")
  output_dir <- here("data/2-cleaned/pubmed")
  file_pattern <- "\\.xml$"
  output_prefix_papers  <- "papers_"
  output_prefix_authors <- "authors_"
  
} else if (data_type == "journal") {
  input_dir  <- here("data/1-raw/pubmed_journal")
  output_dir <- here("data/2-cleaned/pubmed")
  file_pattern <- "\\.xml$"
  output_prefix_papers  <- "journal_papers_"
  output_prefix_authors <- "journal_authors_"
  
} else {
  stop("Invalid data_type. Use 'query' or 'journal'.")
}

dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# FUNCTION: Extract subject from XML
extract_pubmed_data_from_xml <- function(file_path) {
  message("Processing file: ", file_path)
  
  xml_doc <- tryCatch(read_xml(file_path), error = function(e) {
    message("Error reading file: ", file_path, " - ", e$message)
    return(NULL)
  })
  if (is.null(xml_doc)) return(NULL)
  
  pubmed_articles <- xml_find_all(xml_doc, "/PubmedArticleSet/PubmedArticle")
  if (length(pubmed_articles) == 0) return(tibble())
  
  message("Extracting data from ", length(pubmed_articles), " articles...")
  
  map_df(pubmed_articles, function(article_node) {
    pmid <- xml_text(xml_find_first(article_node, ".//PMID"))
    medline_node <- xml_find_first(article_node, ".//MedlineCitation")
    
    title <- xml_text(xml_find_first(medline_node, ".//ArticleTitle"))
    
    abstract <- xml_find_all(medline_node, ".//Abstract/AbstractText") %>%
      xml_text() %>%
      paste(collapse = "--NEW SECTION--")
    
    language <- xml_find_all(medline_node, ".//Language") %>%
      xml_text() %>%
      paste(collapse = ";")
    
    journal_title <- xml_text(xml_find_first(medline_node, ".//Journal/Title"))
    article_type  <- xml_text(xml_find_first(medline_node, ".//PublicationType"))
    
    doi <- xml_text(xml_find_first(article_node, ".//PubmedData/ArticleIdList/ArticleId[@IdType='doi']"))
    
    # --- Improved Publication Date Handling ---
    # Extract PubDate node
    pub_date_node <- xml_find_first(medline_node, ".//PubDate")
    
    pub_year  <- xml_text(xml_find_first(pub_date_node, ".//Year"))
    pub_month <- xml_text(xml_find_first(pub_date_node, ".//Month"))
    pub_day   <- xml_text(xml_find_first(pub_date_node, ".//Day"))
    
    # Handle Month properly
    month_num <- suppressWarnings(as.numeric(pub_month))
    if (is.na(month_num)) {
      month_num <- match(tolower(pub_month), tolower(month.abb))
    }
    if (is.na(month_num)) {
      month_num <- match(tolower(pub_month), tolower(month.name))
    }
    pub_month <- ifelse(!is.na(month_num), sprintf("%02d", month_num), "01")
    
    # Handle Day
    pub_day <- ifelse(pub_day == "" | is.na(pub_day), "01", sprintf("%02d", as.numeric(pub_day)))
    
    # Assemble date safely
    publication_date <- if (!is.na(pub_year) && pub_year != "") {
      suppressWarnings(lubridate::ymd(paste(pub_year, pub_month, pub_day, sep = "-")))
    } else {
      NA_Date_
    }
    
    # --- Static Search Date ---
    search_date <- "2024-04-23"
    
    # --- Improved Keywords Handling ---
    keywords_nodes <- xml_find_all(medline_node, ".//KeywordList/Keyword")
    keywords <- if (length(keywords_nodes) > 0) {
      xml_text(keywords_nodes) %>% paste(collapse = "; ")
    } else {
      NA_character_
    }
    
    # --- MESH Terms Handling ---
    MESH_nodes <- xml_find_all(medline_node, ".//MeshHeadingList/MeshHeading")
    MESH_terms <- if (length(MESH_nodes) > 0) {
      xml_text(MESH_nodes) %>% paste(collapse = "; ")
    } else {
      NA_character_
    }
    
    tibble(
      pmid, doi, title, abstract, language, journal_title, article_type,
      MESH_terms, keywords, publication_date, search_date,
      year = pub_year
    )
  })
}


# FUNCTION: Extract authors
extract_authors_from_xml <- function(file_path) {
  message("Processing authors for file: ", file_path)
  xml_doc <- tryCatch(read_xml(file_path), error = function(e) {
    message("Error reading file: ", file_path, " - ", e$message)
    return(NULL)
  })
  if (is.null(xml_doc)) return(NULL)
  
  medline_nodes <- xml_find_all(xml_doc, "//MedlineCitation")
  
  map_df(medline_nodes, function(medline_node) {
    pmid <- xml_text(xml_find_first(medline_node, ".//PMID"))
    authors <- xml_find_all(medline_node, ".//Author")
    
    map_df(authors, function(author) {
      tibble(
        pmid = pmid,
        last_name = xml_text(xml_find_first(author, ".//LastName")),
        first_name = xml_text(xml_find_first(author, ".//ForeName")),
        initials = xml_text(xml_find_first(author, ".//Initials")),
        affiliation = xml_find_all(author, ".//Affiliation") %>% xml_text() %>% paste(collapse = "[-AFFIL-SEP-]")
      )
    })
  })
}

# FUNCTION: Parse and combine XML files by year
parse_and_combine_by_year <- function(input_directory, output_directory, extract_func, output_prefix) {
  dir.create(output_directory, showWarnings = FALSE, recursive = TRUE)
  
  xml_files <- list.files(input_directory, pattern = "\\.xml$", full.names = TRUE)
  years <- str_extract(xml_files, "\\d{4}") %>% as.numeric() %>% na.omit() %>% unique() %>% sort()
  
  for (year in years) {
    message("Processing year: ", year)
    year_files <- xml_files[grepl(year, xml_files)]
    
    combined_data <- map_dfr(year_files, function(f) {
      tryCatch(extract_func(f), error = function(e) {
        message("Error parsing file: ", f, " - ", e$message)
        NULL
      })
    })
    
    if (nrow(combined_data) > 0) {
      output_file <- file.path(output_directory, glue("{output_prefix}{year}.csv"))
      write_csv(combined_data, output_file)
      message("Saved: ", output_file)
    } else {
      message("No data extracted for year ", year)
    }
  }
}

# ==== Execute Parsing ====
parse_and_combine_by_year(
  input_directory = input_dir,
  output_directory = output_dir,
  extract_func = extract_pubmed_data_from_xml,
  output_prefix = output_prefix_papers
)

parse_and_combine_by_year(
  input_directory = input_dir,
  output_directory = output_dir,
  extract_func = extract_authors_from_xml,
  output_prefix = output_prefix_authors
)

message("All XML parsing and combining completed.")
