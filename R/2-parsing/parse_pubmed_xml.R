# Load required libraries
pacman::p_load(tidyverse, rentrez, xml2, here, glue)

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
  
  map_df(pubmed_articles, function(article_node) {
    pmid <- xml_text(xml_find_first(article_node, ".//PMID"))
    medline_node <- xml_find_first(article_node, ".//MedlineCitation")
    title <- xml_text(xml_find_first(medline_node, ".//ArticleTitle"))
    abstract <- xml_find_all(medline_node, ".//Abstract/AbstractText") %>%
      xml_text() %>% paste(collapse = "--NEW SECTION--")
    language <- xml_find_all(medline_node, ".//Language") %>% xml_text() %>% paste(collapse = ";")
    journal_title <- xml_text(xml_find_first(medline_node, ".//Journal/Title"))
    article_type <- xml_text(xml_find_first(medline_node, ".//PublicationType"))
    doi <- xml_text(xml_find_first(article_node, ".//PubmedData/ArticleIdList/ArticleId[@IdType='doi']"))
    
    pub_year <- xml_text(xml_find_first(medline_node, ".//PubDate/Year"))
    search_date <- as.character(Sys.Date())
    
    tibble(
      pmid, doi, title, abstract, language, journal_title, article_type,
      search_date, year = pub_year
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
  input_directory = here("data/1-raw/pubmed"),
  output_directory = here("data/2-cleaned/pubmed"),
  extract_func = extract_pubmed_data_from_xml,
  output_prefix = "papers_"
)

parse_and_combine_by_year(
  input_directory = here("data/1-raw/pubmed"),
  output_directory = here("data/2-cleaned/pubmed"),
  extract_func = extract_authors_from_xml,
  output_prefix = "pmid_authors_"
)

message("All XML parsing and combining completed.")
