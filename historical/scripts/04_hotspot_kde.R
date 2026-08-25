# ===============================
#  HOTSPOT KDE MAPPING
# ===============================

# ---- Load libraries ----
library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(tibble)
library(forcats)
library(arrow)
library(sf)
library(spatstat.geom)
library(spatstat.explore)
library(lubridate)

# ---- Paths ----
base_dir <- "."
proc_dir <- file.path(base_dir, "dataset", "processed")
sta_dir <- file.path(base_dir, "dataset", "stations")
out_dir <- file.path(base_dir, "output")
fig_dir <- file.path(base_dir, "figures", "spatial")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(fig_dir, showWarnings = FALSE, recursive = TRUE)

# ---- Load processed trips ----
q2 <- read_parquet(file.path(proc_dir, "divvy_trips_2019_Q2_clean.parquet"))
q3 <- read_parquet(file.path(proc_dir, "divvy_trips_2019_Q3_clean.parquet"))
q4 <- read_parquet(file.path(proc_dir, "divvy_trips_2019_Q4_clean.parquet"))

trips <- bind_rows(q2, q3, q4)

# ---- Convert datetime & extract hour ----
trips$start_time <- ymd_hms(trips$start_time)
trips$end_time <- ymd_hms(trips$end_time)

trips$start_hour <- hour(trips$start_time)
trips$end_hour <- hour(trips$end_time)

# ---- Load station geo dataset ----
stations_geo <- read_csv(file.path(sta_dir, "stations_geo.csv"), show_col_types = FALSE)
stations_geo$station_id <- as.numeric(stations_geo$station_id)

# ===============================
# MORNING PICKUP HOTSPOT
# 6AM - 10AM
# ===============================

morning_pickups <- trips %>%
  filter(start_hour >= 6 & start_hour <= 10)

pickup_geo <- morning_pickups %>%
  left_join(stations_geo,
    by = c("from_station_id" = "station_id")
  ) %>%
  filter(!is.na(latitude), !is.na(longitude))

# Convert to sf and project to UTM zone 16
pickup_sf <- st_as_sf(pickup_geo, coords = c("longitude", "latitude"), crs = 4326)
pickup_proj <- st_transform(pickup_sf, crs = 32616) # UTM zone 16N

pickup_coords <- st_coordinates(pickup_proj)

pickup_win <- owin(
  range(pickup_coords[, 1]),
  range(pickup_coords[, 2])
)

pickup_ppp <- ppp(pickup_coords[, 1],
  pickup_coords[, 2],
  window = pickup_win
)

pickup_kde <- density.ppp(pickup_ppp, sigma = 500)

png(file.path(fig_dir, "morning_pickup_hotspot.png"),
  width = 800, height = 600
)

plot(pickup_kde,
  main = "Morning Pickup Hotspot (6AM–10AM)"
)

dev.off()

# ===============================
# AFTERNOON DROP HOTSPOT
# 4PM - 7PM
# ===============================

afternoon_drops <- trips %>%
  filter(end_hour >= 16 & end_hour <= 19)

drop_geo <- afternoon_drops %>%
  left_join(stations_geo,
    by = c("to_station_id" = "station_id")
  ) %>%
  filter(!is.na(latitude), !is.na(longitude))

# Convert to sf and project to UTM zone 16
drop_sf <- st_as_sf(drop_geo, coords = c("longitude", "latitude"), crs = 4326)
drop_proj <- st_transform(drop_sf, crs = 32616)

drop_coords <- st_coordinates(drop_proj)

drop_win <- owin(
  range(drop_coords[, 1]),
  range(drop_coords[, 2])
)

drop_ppp <- ppp(drop_coords[, 1],
  drop_coords[, 2],
  window = drop_win
)

drop_kde <- density.ppp(drop_ppp, sigma = 500)

png(file.path(fig_dir, "afternoon_drop_hotspot.png"),
  width = 800, height = 600
)

plot(drop_kde,
  main = "Afternoon Drop Hotspot (4PM–7PM)"
)

dev.off()

# ===============================
# DONE
# ===============================
cat("KDE hotspot figures saved to:", fig_dir, "\n")
