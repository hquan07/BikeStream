library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(tibble)
library(forcats)
library(janitor)
library(arrow)

# paths — set project root (run from project root or scripts/ subfolder)
base_dir <- "."
raw_dir <- file.path(base_dir, "dataset", "raw")
proc_dir <- file.path(base_dir, "dataset", "processed")
sta_dir <- file.path(base_dir, "dataset", "stations")

dir.create(proc_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(sta_dir, showWarnings = FALSE, recursive = TRUE)

# helper: standardise one data frame
standardise <- function(df) {
  df %>%
    mutate(
      # duration comes in as character with commas ("1,048.0") — fix
      tripduration     = as.numeric(gsub(",", "", as.character(tripduration))),
      trip_id          = as.integer(trip_id),
      bikeid           = as.integer(bikeid),
      from_station_id  = as.integer(from_station_id),
      to_station_id    = as.integer(to_station_id),
      start_time       = as.POSIXct(start_time, format = "%Y-%m-%d %H:%M:%S"),
      end_time         = as.POSIXct(end_time, format = "%Y-%m-%d %H:%M:%S"),
      birthyear        = as.integer(birthyear)
    ) %>%
    # drop rows where station IDs are missing (unusable for spatial analysis)
    filter(!is.na(from_station_id), !is.na(to_station_id)) %>%
    # remove exact duplicate rows
    distinct()
}

# 1. Read Q2
cat("Reading Q2 ...\n")
q2 <- read_csv(file.path(raw_dir, "Divvy_Trips_2019_Q2.csv"),
  show_col_types = FALSE
) %>%
  rename(
    trip_id = `01 - Rental Details Rental ID`,
    start_time = `01 - Rental Details Local Start Time`,
    end_time = `01 - Rental Details Local End Time`,
    bikeid = `01 - Rental Details Bike ID`,
    tripduration = `01 - Rental Details Duration In Seconds Uncapped`,
    from_station_id = `03 - Rental Start Station ID`,
    from_station_name = `03 - Rental Start Station Name`,
    to_station_id = `02 - Rental End Station ID`,
    to_station_name = `02 - Rental End Station Name`,
    usertype = `User Type`,
    gender = `Member Gender`,
    birthyear = `05 - Member Details Member Birthday Year`
  )

# 2. Read Q3
cat("Reading Q3 ...\n")
q3 <- read_csv(file.path(raw_dir, "Divvy_Trips_2019_Q3.csv"),
  show_col_types = FALSE
)

# 3. Read Q4
cat("Reading Q4 ...\n")
q4 <- read_csv(file.path(raw_dir, "Divvy_Trips_2019_Q4.csv"),
  show_col_types = FALSE
)

# 4. Standardise each quarter
cat("Standardising Q2 ...\n")
q2_clean <- standardise(q2)
cat("Standardising Q3 ...\n")
q3_clean <- standardise(q3)
cat("Standardising Q4 ...\n")
q4_clean <- standardise(q4)

# 5. Extract station lookup
cat("Building station lookup ...\n")
stations <- bind_rows(
  q2_clean %>% select(station_id = from_station_id, station_name = from_station_name),
  q2_clean %>% select(station_id = to_station_id, station_name = to_station_name),
  q3_clean %>% select(station_id = from_station_id, station_name = from_station_name),
  q3_clean %>% select(station_id = to_station_id, station_name = to_station_name),
  q4_clean %>% select(station_id = from_station_id, station_name = from_station_name),
  q4_clean %>% select(station_id = to_station_id, station_name = to_station_name)
) %>%
  distinct() %>%
  arrange(station_id)

# 5b. Join station coordinates from Divvy_Stations_2017_Q3Q4.csv
cat("Joining station coordinates ...\n")
station_coords <- read_csv(
  file.path(raw_dir, "Divvy_Stations_2017_Q3Q4.csv"),
  show_col_types = FALSE
) %>%
  select(station_id = id, latitude, longitude) %>%
  distinct(station_id, .keep_all = TRUE)

# Join: keep one row per station_id (first name), then attach lat/lon
stations_geo <- stations %>%
  distinct(station_id, .keep_all = TRUE) %>%
  left_join(station_coords, by = "station_id")

cat(
  "  Stations with coords:",
  sum(!is.na(stations_geo$latitude)), "/", nrow(stations_geo), "\n"
)
cat(
  "  Stations missing coords:",
  sum(is.na(stations_geo$latitude)), "\n"
)

# 6. Write outputs
cat("Writing cleaned datasets ...\n")
write_parquet(q2_clean, file.path(proc_dir, "divvy_trips_2019_Q2_clean.parquet"))
write_parquet(q3_clean, file.path(proc_dir, "divvy_trips_2019_Q3_clean.parquet"))
write_parquet(q4_clean, file.path(proc_dir, "divvy_trips_2019_Q4_clean.parquet"))
write_csv(stations, file.path(sta_dir, "stations.csv"))
write_csv(stations_geo, file.path(sta_dir, "stations_geo.csv"))

# 7. Verification summary
cat("\n VERIFICATION \n")
cat("Q2 rows:", nrow(q2_clean), "\n")
cat("Q3 rows:", nrow(q3_clean), "\n")
cat("Q4 rows:", nrow(q4_clean), "\n")
cat("Total  :", nrow(q2_clean) + nrow(q3_clean) + nrow(q4_clean), "\n")
cat("Unique stations:", nrow(stations), "\n")
cat("Stations with geo:", sum(!is.na(stations_geo$latitude)), "\n")
cat("Stations missing geo:", sum(is.na(stations_geo$latitude)), "\n")

cat("\n-- Column types (Q2 as sample) --\n")
str(q2_clean)

cat("\n-- NA check (Q2 as sample) --\n")
print(colSums(is.na(q2_clean)))

cat("\n-- Output files --\n")
cat(list.files(proc_dir, full.names = TRUE), sep = "\n")
cat("\n")
cat(list.files(sta_dir, full.names = TRUE), sep = "\n")
cat("\nDone!\n")
