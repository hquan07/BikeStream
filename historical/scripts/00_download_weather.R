library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(tibble)
library(forcats)
library(arrow)

base_dir <- "."

ext_dir <- file.path(base_dir, "dataset", "external")
dir.create(ext_dir, showWarnings = FALSE, recursive = TRUE)

weather_file <- file.path(ext_dir, "chicago_weather_2019.parquet")

cat("Downloading historical weather data for Chicago (2019) from Open-Meteo...\n")

lat <- 41.8781
lon <- -87.6298
start_date <- "2019-04-01"
end_date <- "2019-12-31"

url <- sprintf("https://archive-api.open-meteo.com/v1/archive?latitude=%f&longitude=%f&start_date=%s&end_date=%s&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=America%%2FChicago&format=csv", lat, lon, start_date, end_date)

tmp <- tempfile()
download.file(url, tmp, quiet = TRUE)

# Read skipping the first 3 lines (open-meteo header)
weather_raw <- read.csv(tmp, skip = 3)
weather_df <- tibble(
  date = as.Date(weather_raw$time),
  temp_max = weather_raw$temperature_2m_max...C.,
  temp_min = weather_raw$temperature_2m_min...C.,
  precip = weather_raw$precipitation_sum..mm.
)

write_parquet(weather_df, weather_file)
cat("Weather data downloaded and saved to", weather_file, "\n")
