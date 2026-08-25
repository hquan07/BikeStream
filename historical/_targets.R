library(targets)
library(tarchetypes)

list(
  tar_target(
    divvy_raw_data,
    {
      source("scripts/00_download_divvy.R")
      list.files("dataset/raw", pattern = "\\.csv$", full.names = TRUE)
    },
    format = "file"
  ),
  tar_target(
    weather_data,
    {
      source("scripts/00_download_weather.R")
      "dataset/external/chicago_weather_2019.parquet"
    },
    format = "file"
  ),
  tar_target(
    clean_data,
    {
      divvy_raw_data
      source("scripts/01_merge_clean.R")
      c(
        "dataset/processed/divvy_trips_2019_Q2_clean.parquet",
        "dataset/processed/divvy_trips_2019_Q3_clean.parquet",
        "dataset/processed/divvy_trips_2019_Q4_clean.parquet",
        "dataset/stations/stations.csv",
        "dataset/stations/stations_geo.csv"
      )
    },
    format = "file"
  ),
  tar_target(
    temporal_data,
    {
      # Ensure it depends on clean_data
      clean_data
      source("scripts/02_temporal_analysis.R")
      list.files("figures/temporal", full.names = TRUE)
    },
    format = "file"
  ),
  tar_target(
    od_flow,
    {
      clean_data
      source("scripts/03_od_matrix_flow.R")
      list.files("output", full.names = TRUE)
    },
    format = "file"
  ),
  tar_target(
    hotspot_maps,
    {
      clean_data
      source("scripts/04_hotspot_kde.R")
      list.files("figures/spatial", full.names = TRUE)
    },
    format = "file"
  ),
  tar_target(
    network_analysis,
    {
      clean_data
      source("scripts/05_network_analysis.R")
      list.files("figures/network", full.names = TRUE)
    },
    format = "file"
  ),
  tar_target(
    forecasting,
    {
      clean_data
      source("scripts/06_forecasting.R")
      list.files("figures/forecast", full.names = TRUE)
    },
    format = "file"
  )
)
