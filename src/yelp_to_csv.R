library(jsonlite)
library(magrittr)
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
library(purrr)
library(tibble)

# extract functions ----
extract2_with_na <- function(x, y) if (is.null(x[[y]])) NA else x[[y]]
extract_with_na <- Vectorize(extract2_with_na, vectorize.args = "y", SIMPLIFY = FALSE)

# Data set 1 ----
# names and unique keys for each search (city + category)
cities <- read_lines("data/cities.txt")
categories <- read_lines("data/categories.txt")
search_keys <- 
  expand.grid(search_city = cities, search_category = categories, 
              stringsAsFactors = FALSE) %>%
  as_tibble() %>%
  mutate(search_key = map2_chr(search_city, search_category, function(city, category) {
    city %>% 
      str_replace_all(pattern = " ", replacement = "_") %>%
      str_replace_all(pattern = ",", replacement = "") %>%
      str_to_lower() %>%
      str_c(category, sep = "_")
  }))

# Data set 2 ----
# yelp businesses with keys for searches

# full file names to read in files
yelp_files <- list.files(path = "data/raw", pattern = "*.json", 
                         full.names = TRUE)

# short file names, truncated, to create object names
# these are *not* unique! cities with > 20 businesses 
# have multiple associated json objects
yelp_search_keys <- 
  list.files(path = "data/raw", pattern = "*.json", 
             full.names = FALSE) %>%
  str_replace("_[0-9].*", "")

# read responses ----
responses <-   
  yelp_files %>%
  map(read_file) %>%
  map(fromJSON, simplifyDataFrame = FALSE) %>%
  set_names(yelp_search_keys)

# search keys and ids ----
search_key_and_id <- 
  responses %>%
  map("businesses") %>%
  map(~map(., "id")) %>%
  map(~tibble(id = flatten_chr(.))) %>%
  bind_rows(.id = "search_key")

# basic restaurant information ----
basic_restaurant_information <- 
  responses %>%
  map("businesses") %>%
  flatten() %>%
  compact() %>%
  map_dfr(extract_with_na, 
          c("id", "name", "price", "rating", "review_count", "url")) %>%
  mutate(search_key = search_key_and_id$search_key)


# categories ----
restaurant_categories <- 
  responses %>%
  map("businesses") %>%
  flatten() %>%
  map("categories") %>% 
  set_names(search_key_and_id$id) %>%
  map(~map_df(., as_tibble)) %>%
  bind_rows(.id = "id")

# locations ----

restaurant_coordinates <- 
  responses %>%
  map("businesses") %>%
  flatten() %>%
  map("coordinates") %>% 
  set_names(search_key_and_id$id) %>%
  # compact() %>%
  map_dfr(extract_with_na, c("latitude", "longitude"), .id = "id") %>%
  mutate(search_key = search_key_and_id$search_key)

# restaurant_locations <- 
#   responses %>%
#   map("businesses") %>%
#   flatten() %>%
#   map("location") %>%
#   set_names(search_key_and_id$id) %>%
#   map_dfr(function(x) {
#     data_frame(
#       address = x$address1, 
#       city = x$city, 
#       zip_code = x$zip_code, 
#       country = x$country
#     )
#   }, .id = "id")

restaurant_locations <-
  responses %>%
  map("businesses") %>%
  flatten() %>%
  map("location") %>%
  set_names(search_key_and_id$id) %>%
  map_dfr(extract_with_na,
          c("address1", "city", "state", "zip_code", "country"), 
          .id = "id") %>%
 mutate(search_key = search_key_and_id$search_key)

# join ----
# unit of observation: restaurant
restaurants <- 
  basic_restaurant_information %>% 
  left_join(restaurant_locations, by = c("id", "search_key")) %>%
  left_join(restaurant_coordinates, by = c("id", "search_key")) %>%
  left_join(search_keys, by = "search_key")
  
# unit of observation: restaurant X category
# TODO: dimension doesn't work out! 4791 vs 4800 ... 
# even after using distinct both times, 4073 vs 4070

unique_restaurants <- 
  restaurants %>%
  select(-search_city, -search_key, -search_category) %>%
  distinct()

restaurants_by_category <- 
  restaurant_categories %>%
  distinct() %>%
  left_join(unique_restaurants, by = "id")

# map ----
library(leaflet)
leaflet(unique_restaurants) %>%
  addTiles() %>%
  addCircleMarkers(label = ~name, 
                   clusterOptions = markerClusterOptions())

leaflet(unique_restaurants) %>%
  addTiles() %>%
  addCircles(label = ~name)

# summary ----

# restaurants per city, duplicates removed
restaurants %>% select(id, city) %>% distinct() %>% group_by(city) %>% count()

# Seattle only, no duplicate handling
seattle_restaurant_categories <- 
  restaurants %>%
  filter(city == "Seattle") %>%
  group_by(city, search_category) %>%
  count() %>%
  ungroup()

write_csv(seattle_restaurant_categories, "data/seattle_restaurant_categories.csv")

# by zip, no duplicate handling
restaurants %>%
  filter(city == "Seattle") %>%
  group_by(city, postal_code, search_category) %>%
  count() 

# Seattle restaurants, no duplicate handling
seattle_restaurants <- 
  restaurants %>%
  filter(city == "Seattle") 

write_csv(seattle_restaurants, "data/seattle_restaurants.csv")
