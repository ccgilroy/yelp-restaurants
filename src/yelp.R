library(dplyr)
library(httr)
library(jsonlite)
library(purrr)
library(readr)
library(stringr)
library(yaml)

## Set up API and oauth --------------------------------------------------------

## read api key from a YAML file, which *isn't* committed to the git repo
keys <- yaml.load_file("src/yelp.yml")
api_key = keys$api_key

## yelp API v3 business search url
yelp_search <- "https://api.yelp.com/v3/businesses/search"

## read in list of cities
cities <- read_lines("data/cities.txt")

## read in list of categories
categories <- read_lines("data/categories.txt")

request_params <- 
  expand.grid(city = cities, category = categories, 
              stringsAsFactors = FALSE) %>%
  as_tibble()

## Main function for making requests -------------------------------------------
## Note presence of hard-coded values like sleep time (10) and the file path
## as well as global variables like yelp_search and api_key as defaults

make_yelp_request <- function(city, category, offset = 0, 
                              url = yelp_search, 
                              key = api_key) {
  ## pause between API requests to avoid rate-limiting
  Sys.sleep(10)
  
  ## make request
  query_list <-
    list(
      categories = category, 
      location = city, 
      offset = offset
    )
  ## authentication using header is explained here:
  ## https://www.yelp.com/developers/documentation/v3/authentication
  r <- GET(yelp_search, 
           add_headers(Authorization = str_c("Bearer ", key)), 
           query = query_list)
  
  ## write response to json file
  ## only if http status is ok
  if(status_code(r) == 200) {
    file_name <-
      city %>% 
      str_replace_all(pattern = " ", replacement = "_") %>%
      str_replace_all(pattern = ",", replacement = "") %>%
      str_to_lower() %>%
      str_c(category, offset, Sys.Date(), sep = "_") %>%
      str_c(".json")
    content(r, as = "text") %>% 
      prettify() %>% 
      write_file(file.path("data", "raw", file_name))
  }
  
  ## return response
  r  
}

## Make requests ---------------------------------------------------------------

pmap(request_params, function(city, category) {
  
  ## make first request (default offset = 0)
  r <- make_yelp_request(city, category)
  
  ## only parse if status is ok
  if(status_code(r) == 200) {
    r_parsed <- content(r)
    
    ## Are there more than 20 listings?
    if (r_parsed$total > 20) {
      ## calculate number of offsets necessary
      max_offsets <- floor(r_parsed$total / 20) 
      
      ## make requests with offsets
      r_offsets <- 
        lapply(1:max_offsets, function(offset) {
          make_yelp_request(city, category, offset*20)
        })
      
      ## return list of responses if > 20 listings
      c(list(r), r_offsets)
    } else {
      ## return single response if <= 20 listings
      r
    }
  } else {
    ## return single response if http error
    r
  }
}) -> responses 
## assign responses to object for manual inspection if necessary

## save as R object for request information
saveRDS(responses, "data/raw/responses.rds")


