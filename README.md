This project retrieves data from the Yelp API for restaurants by location
and by category. It turns that data from a series of json files into
csv files at a variety of levels of aggregation---search-level,
restaurant-level, city or zip-code level---to facilitate mapping, 
visualization, and analysis.

You'll need an API key saved in src/yelp.yml to use this code. 
Unfortunately, Yelp no longer allows signups for v2 of their API.
This code still needs to be updated for v3, which returns a slightly
different data structure. 
