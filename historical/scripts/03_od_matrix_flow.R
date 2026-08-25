## Phase 3 - OD Matrix & Flow Map
## Divvy Bike Share (Chicago 2019)

# Packages
library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(tibble)
library(forcats)
library(arrow)
library(lubridate)
library(leaflet)
library(htmlwidgets)


# Paths
base_dir <- "."

proc_dir <- file.path(base_dir, "dataset", "processed")
sta_dir <- file.path(base_dir, "dataset", "stations")
out_dir <- file.path(base_dir, "output")

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# Load + clean trips
trips <- bind_rows(
  read_parquet(file.path(proc_dir, "divvy_trips_2019_Q2_clean.parquet")),
  read_parquet(file.path(proc_dir, "divvy_trips_2019_Q3_clean.parquet")),
  read_parquet(file.path(proc_dir, "divvy_trips_2019_Q4_clean.parquet"))
) %>%
  mutate(
    start_time = ymd_hms(start_time, quiet = TRUE),
    start_hour = hour(start_time)
  ) %>%
  filter(
    !is.na(from_station_id),
    !is.na(to_station_id),
    from_station_id != to_station_id
  )

cat("Total trips loaded:", nrow(trips), "\n")
print(head(trips, 5))

# 1) OD matrix by hour
od_by_hour <- trips %>%
  group_by(from_station_id, to_station_id, start_hour) %>%
  summarise(n_trips = n(), .groups = "drop")

write_csv(od_by_hour, file.path(out_dir, "od_matrix_by_hour.csv"))

hourly_summary <- od_by_hour %>%
  group_by(start_hour) %>%
  summarise(
    total_trips = sum(n_trips),
    n_od_pairs = n(),
    .groups = "drop"
  )

write_csv(hourly_summary, file.path(out_dir, "hourly_summary.csv"))

ggplot(hourly_summary, aes(start_hour, total_trips)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  labs(
    title = "Total trips by hour",
    x = "Hour of day",
    y = "Number of trips"
  ) +
  theme_minimal()

# 2) OD matrix by timeframe
trips <- trips %>%
  mutate(
    time_frame = case_when(
      start_hour >= 6 & start_hour < 12 ~ "Morning",
      start_hour >= 12 & start_hour < 18 ~ "Noon",
      TRUE ~ "Evening"
    )
  )

od_by_frame <- trips %>%
  group_by(from_station_id, to_station_id, time_frame) %>%
  summarise(n_trips = n(), .groups = "drop")

write_csv(od_by_frame, file.path(out_dir, "od_matrix_by_timeframe.csv"))

frame_summary <- od_by_frame %>%
  group_by(time_frame) %>%
  summarise(
    total_trips = sum(n_trips),
    n_od_pairs = n(),
    .groups = "drop"
  )

write_csv(frame_summary, file.path(out_dir, "timeframe_summary.csv"))
print(frame_summary)

ggplot(
  frame_summary,
  aes(factor(time_frame, levels = c("Morning", "Noon", "Evening")), total_trips, fill = time_frame)
) +
  geom_col(show.legend = FALSE) +
  scale_fill_manual(values = c("Morning" = "#2ecc71", "Noon" = "#f39c12", "Evening" = "#9b59b6")) +
  labs(
    title = "Trips by time frame",
    x = "Time frame",
    y = "Number of trips"
  ) +
  theme_minimal()

# Station coordinates — use stations_geo.csv from script 01
stations_geo <- read_csv(file.path(sta_dir, "stations_geo.csv"), show_col_types = FALSE) %>%
  rename(lat = latitude, lon = longitude) %>%
  distinct(station_id, .keep_all = TRUE) %>%
  filter(!is.na(lat), !is.na(lon))

cat("Stations with coordinates:", nrow(stations_geo), "\n")

# 3) Build flow map
od_flows <- trips %>%
  group_by(from_station_id, to_station_id) %>%
  summarise(n_trips = n(), .groups = "drop") %>%
  arrange(desc(n_trips)) %>%
  slice_head(n = 150) %>%
  left_join(
    stations_geo %>% select(from_station_id = station_id, from_lat = lat, from_lon = lon),
    by = "from_station_id"
  ) %>%
  left_join(
    stations_geo %>% select(to_station_id = station_id, to_lat = lat, to_lon = lon),
    by = "to_station_id"
  ) %>%
  filter(!is.na(from_lat), !is.na(to_lat))

pal <- colorNumeric("YlOrRd", domain = od_flows$n_trips)

m <- leaflet() %>%
  addTiles() %>%
  setView(lng = -87.63, lat = 41.88, zoom = 11)

if (nrow(od_flows) > 0) {
  for (i in seq_len(nrow(od_flows))) {
    r <- od_flows[i, ]
    m <- m %>%
      addPolylines(
        lng = c(r$from_lon, r$to_lon),
        lat = c(r$from_lat, r$to_lat),
        weight = pmin(8, 1 + log1p(r$n_trips)),
        opacity = 0.6,
        color = pal(r$n_trips),
        popup = paste0("From ", r$from_station_id, " -> To ", r$to_station_id, "<br>Trips: ", r$n_trips)
      )
  }
} else {
  message("No OD flows found to draw on the map.")
}

m <- m %>%
  addCircleMarkers(
    data = stations_geo,
    lng = ~lon,
    lat = ~lat,
    radius = 2,
    color = "navy",
    fillOpacity = 0.5
  )

print(m)
saveWidget(m, file.path(out_dir, "flow_map.html"), selfcontained = FALSE)

cat("\nAll done. Files saved in:", out_dir, "\n")
