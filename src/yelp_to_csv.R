library(jsonlite)
library(magrittr)
library(readr)
library(stringr)
library(dplyr)
library(tidyr)
library(purrr)
library(tibble)

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
  map_dfr(magrittr::extract, 
          c("id", "name", "rating", "review_count", "url")) %>%
  mutate(search_key = search_key_and_id$search_key)

# categories ----
restaurant_categories <- 
  responses %>%
  map("businesses") %>%
  flatten() %>%
  map("categories") %>% 
  set_names(search_key_and_id$id) %>%
  map(as_tibble) %>%
  bind_rows(.id = "id") %>%
  set_colnames(c("id", "category_description", "category"))

# locations ----
restaurant_locations <- 
  responses %>%
  map("businesses") %>%
  flatten() %>%
  map("location") %>%
  set_names(search_key_and_id$id) %>%
  map_dfr(function(x) {
    address <- if (length(x$address) > 0) str_c(x$address, collapse = ", ") else NA
    zip <- if (!is.null(x$postal_code)) x$postal_code  else NA
    lat <- if (!is.null(x$coordinate$latitude)) x$coordinate$latitude else NA
    lon <- if (!is.null(x$coordinate$longitude)) x$coordinate$longitude else NA
    data_frame(
      address = address, 
      city = x$city, 
      state_code = x$state_code,
      postal_code = zip,
      country_code = x$country_code, 
      latitude = lat, 
      longitude = lon
    ) 
  }, .id = "id") %>%
  mutate(search_key = search_key_and_id$search_key)


# join ----
# unit of observation: restaurant
restaurants <- 
  basic_restaurant_information %>% 
  left_join(restaurant_locations, by = c("id", "search_key")) %>%
  left_join(search_keys, by = "search_key")
  
# unit of observation: restaurant X category
# TODO: dimension doesn't work out! 4791 vs 4800 ... 
# even after using distinct both times, 4073 vs 4070

unique_restaurants <- 
  basic_restaurant_information %>% 
  left_join(restaurant_locations, by = c("id", "search_key")) %>%
  select(-search_key) %>%
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
