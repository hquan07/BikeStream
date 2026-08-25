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
library(tsibble)
library(feasts)
library(fable)
library(fabletools)
library(fable.prophet)

base_dir <- "."
proc_dir <- file.path(base_dir, "dataset", "processed")

read_quarter <- function(quarter) {
  path <- file.path(proc_dir, paste0("divvy_trips_2019_", quarter, "_clean.parquet"))
  if (!file.exists(path)) stop("File not found: ", path, "\nRun 01_merge_clean.R first.")
  read_parquet(path) |>
    mutate(start_time = as.POSIXct(start_time,
      format = "%Y-%m-%d %H:%M:%S", tz = "UTC"
    ))
}

trips <- bind_rows(
  read_quarter("Q2"),
  read_quarter("Q3"),
  read_quarter("Q4")
)

head(trips)

cat("Total trips loaded:", nrow(trips), "\n")
cat(
  "Date range:", as.character(min(as_date(trips$start_time))),
  "to", as.character(max(as_date(trips$start_time))), "\n"
)

# Comment:
# The dataset covers Divvy bike-share trips across Q2-Q4 2019 (April to December),
# totalling over 3.4 million individual rides. Each row records one trip with its
# start/end times, station IDs, and rider attributes. We will aggregate these to
# daily trip counts for time-series modelling.

# 2. Time-Series Forecasting
# 2.1 Total Demand

# Build daily total demand
weather_path <- file.path(base_dir, "dataset", "external", "chicago_weather_2019.parquet")
if(file.exists(weather_path)) {
  weather_data <- read_parquet(weather_path)
} else {
  weather_data <- tibble(date = Sys.Date(), temp_max = 0, precip = 0)[0,]
}

daily_total <- trips |>
  mutate(date = as_date(start_time)) |>
  count(date, name = "n_trips") |>
  arrange(date) |>
  left_join(weather_data, by = "date") |>
  fill(temp_max, precip, .direction = "downup")

head(daily_total)

ts_total <- as_tsibble(daily_total, index = date)

autoplot(ts_total, n_trips) +
  labs(
    title = "Daily Total Bike-Share Demand (Q2-Q4 2019)",
    x = "Date", y = "Number of Trips"
  ) +
  theme_minimal(base_size = 13)

# Comment:
# The total daily demand shows strong seasonality. Trip counts peak in summer
# (June-August), reaching over 20,000 rides per day, and decline sharply through
# autumn into winter. A clear weekly pattern is also visible throughout the series
# - weekdays consistently attract more trips than weekends, reflecting commuter
# usage. These seasonal and weekly structures motivate the use of ARIMA and ETS
# models that can capture such patterns.

# Train / test split
cutoff_date <- max(daily_total$date) - days(13)

train_total <- filter(ts_total, date < cutoff_date)
test_total <- filter(ts_total, date >= cutoff_date)
h <- nrow(test_total)

cat(
  "Train period:", as.character(min(daily_total$date)),
  "to", as.character(cutoff_date - 1), "\n"
)
cat(
  "Test period :", as.character(cutoff_date),
  "to", as.character(max(daily_total$date)), "\n"
)
cat("Forecast horizon:", h, "days\n")

# Comment:
# The last 14 days of Q4 (mid-to-late December 2019) are held out as the test set.
# This window captures the holiday period - a natural out-of-sample challenge where
# demand behaviour can shift significantly from the training pattern.

# Fit ARIMA, ETS, and PROPHET models
fit_total <- train_total |>
  model(
    ARIMA = ARIMA(n_trips),
    ETS   = ETS(n_trips),
    PROPHET = prophet(n_trips ~ temp_max + precip + season("week", type = "additive"))
  )

fit_total

# Comment:
# - ARIMA(0,1,2)(0,0,2)[7]: The model applies first-order differencing (d = 1) to
#   remove the trend, uses 2 moving-average terms for the non-seasonal component,
#   and 2 seasonal moving-average terms at lag 7 to capture the weekly cycle.
# - ETS(A,N,A): An additive-error, no-trend, additive-seasonal model. The absence
#   of a trend component (N) is consistent with a stationary mean after the seasonal
#   adjustment; the seasonal period is set automatically to 7 days.

# 2.2 Cluster-Level Demand

# Assign station clusters
all_stations <- sort(unique(c(trips$from_station_id, trips$to_station_id)))
brks <- quantile(all_stations, probs = c(0, 0.25, 0.50, 0.75, 1.0))

station_cluster <- tibble(station_id = all_stations) |>
  mutate(cluster = cut(station_id,
    breaks = brks,
    labels = paste0("C", 1:4), include.lowest = TRUE
  ))

daily_cluster <- trips |>
  mutate(date = as_date(start_time)) |>
  left_join(station_cluster, by = c("from_station_id" = "station_id")) |>
  filter(!is.na(cluster)) |>
  count(date, cluster, name = "n_trips") |>
  arrange(cluster, date)

# Fill every cluster x date combination (0 for missing days)
full_grid <- expand_grid(
  date    = seq(min(daily_cluster$date), max(daily_cluster$date), by = "1 day"),
  cluster = unique(daily_cluster$cluster)
)

daily_cluster_full <- full_grid |>
  left_join(daily_cluster, by = c("date", "cluster")) |>
  left_join(weather_data, by = "date") |>
  replace_na(list(n_trips = 0L)) |>
  fill(temp_max, precip, .direction = "downup")

head(daily_cluster_full)

# Comment:
# Stations are partitioned into four clusters (C1-C4) by splitting the sorted
# station ID range into equal quartiles. This provides a reproducible, balanced
# grouping that approximates a spatial division: earlier-assigned station IDs tend
# to correspond to the original city-centre network, while higher IDs reflect
# later-added stations in outer areas. Each cluster is then assigned its own daily
# demand series.

ts_cluster <- as_tsibble(daily_cluster_full, index = date, key = cluster)

autoplot(ts_cluster, n_trips) +
  facet_wrap(~cluster, scales = "free_y", ncol = 2) +
  labs(
    title = "Daily Demand by Cluster (Q2-Q4 2019)",
    x = "Date", y = "Number of Trips", colour = "Cluster"
  ) +
  theme_minimal(base_size = 12)

# Comment:
# All four clusters mirror the aggregate seasonal trend - peaking in summer and
# dropping in winter - confirming that the pattern is system-wide rather than
# localised. However, the scale differs markedly: C1 and C2 (likely denser,
# central zones) generate far more trips per day than C3 and C4. The weekly rhythm
# is visible across all clusters, though it is most pronounced in the high-volume
# clusters.

# Fit ARIMA and ETS models (cluster level)
train_cluster <- filter(ts_cluster, date < cutoff_date)
test_cluster <- filter(ts_cluster, date >= cutoff_date)

fit_cluster <- train_cluster |>
  model(
    ARIMA = ARIMA(n_trips),
    ETS   = ETS(n_trips),
    PROPHET = prophet(n_trips ~ temp_max + precip + season("week", type = "additive"))
  )

fit_cluster

# Comment:
# Each cluster receives its own independently fitted model. C1 and C2 settle on
# the same specification as the total-demand model ARIMA(0,1,2)(0,0,2)[7],
# reflecting similar dynamics driven by high trip volumes. C3 and C4, with lower
# and more volatile counts, are assigned slightly different seasonal ARIMA orders,
# and C3's ETS drops to a simple exponential smoother ETS(A,N,N) - indicating a
# weaker seasonal signal at that scale.

# 3. Models
# 3.1 ARIMA

# Total demand forecast - ARIMA
fc_arima_total <- fit_total |>
  select(ARIMA) |>
  forecast(new_data = test_total)

fc_arima_total |>
  autoplot(ts_total, level = 80) +
  labs(
    title = "Total Demand Forecast - ARIMA",
    subtitle = paste0("Horizon: ", h, " days (Dec 2019 test window)"),
    x = "Date", y = "Number of Trips"
  ) +
  theme_minimal(base_size = 13)

# Comment:
# The ARIMA forecast tracks the downward trajectory of December demand and
# preserves the weekly oscillation. The 80% prediction interval (shaded band)
# widens modestly over the 14-day horizon, reflecting uncertainty that grows with
# lead time but remains relatively tight - a sign that the model has captured the
# dominant structure of the series.

# Cluster-level forecast - ARIMA
fc_arima_cluster <- fit_cluster |>
  select(ARIMA) |>
  forecast(new_data = test_cluster)

fc_arima_cluster |>
  autoplot(ts_cluster, level = 80) +
  facet_wrap(~cluster, scales = "free_y", ncol = 2) +
  labs(
    title = "Cluster-Level Demand Forecast - ARIMA",
    subtitle = paste0("Horizon: ", h, " days (Dec 2019 test window)"),
    x = "Date", y = "Number of Trips", colour = "Cluster"
  ) +
  theme_minimal(base_size = 12)

# Comment:
# At the cluster level, ARIMA preserves the expected shape for C1 and C2 - a
# gently declining forecast with visible weekly oscillations. For C3 and C4, where
# volumes are smaller, the forecast intervals are proportionally wider relative to
# the point estimates, reflecting greater uncertainty at finer granularity. In all
# four panels the model correctly anticipates that demand continues to fall through
# the holiday window.

# 3.2 ETS

# Total demand forecast - ETS
fc_ets_total <- fit_total |>
  select(ETS) |>
  forecast(new_data = test_total)

fc_ets_total |>
  autoplot(ts_total, level = 80) +
  labs(
    title = "Total Demand Forecast - ETS",
    subtitle = paste0("Horizon: ", h, " days (Dec 2019 test window)"),
    x = "Date", y = "Number of Trips"
  ) +
  theme_minimal(base_size = 13)

# Comment:
# The ETS model produces a smoother forecast trajectory compared to ARIMA. Without
# an explicit trend component (the "N" in ETS(A,N,A)), the level adapts adaptively
# through exponential smoothing of recent observations. The seasonal pattern is
# additive and fixed from the training window. As a result, ETS tends to
# underestimate the continued decline in demand over the holiday period.

# Cluster-level forecast - ETS
fc_ets_cluster <- fit_cluster |>
  select(ETS) |>
  forecast(new_data = test_cluster)

fc_ets_cluster |>
  autoplot(ts_cluster, level = 80) +
  facet_wrap(~cluster, scales = "free_y", ncol = 2) +
  labs(
    title = "Cluster-Level Demand Forecast - ETS",
    subtitle = paste0("Horizon: ", h, " days (Dec 2019 test window)"),
    x = "Date", y = "Number of Trips", colour = "Cluster"
  ) +
  theme_minimal(base_size = 12)

# Comment:
# Across all four clusters, ETS produces flatter forecasts than ARIMA. The weekly
# seasonal component is reproduced, but the level remains close to the end of the
# training series rather than tracking further decline. This is most evident in C1,
# where ETS overshoots actual December demand. C3, with its simple ETS(A,N,N)
# specification, produces a nearly flat forecast - reasonable given the low and
# noisy trip counts in that cluster.

# Side-by-side comparison (ARIMA vs ETS)
fc_all_total <- fit_total |> forecast(new_data = test_total)

fc_all_total |>
  autoplot(ts_total, level = NULL) +
  labs(
    title = "Total Demand - ARIMA vs ETS",
    x = "Date", y = "Number of Trips", colour = "Model"
  ) +
  theme_minimal(base_size = 13)

fc_all_cluster <- fit_cluster |> forecast(new_data = test_cluster)

fc_all_cluster |>
  autoplot(ts_cluster, level = NULL) +
  facet_wrap(~cluster, scales = "free_y", ncol = 2) +
  labs(
    title = "Cluster-Level Demand - ARIMA vs ETS",
    x = "Date", y = "Number of Trips", colour = "Model"
  ) +
  theme_minimal(base_size = 12)

# Comment:
# When plotted together, the divergence between the two models is most apparent in
# the final days of the forecast window. ARIMA (following first-order differencing)
# continues to drift downward, aligning more closely with the observed holiday
# drop-off. ETS levels off sooner and stays higher, producing a systematic
# overestimate in late December. At the cluster level, the difference is most
# pronounced in C1 and C2, where the volume is large enough for the divergence to
# be clearly visible.

# 4. Assessment
# 4.1 RMSE

# Total demand - RMSE
acc_total <- accuracy(fc_all_total, test_total) |>
  select(.model, RMSE, MAPE)

acc_total

# Comment:
# ARIMA achieves a lower RMSE (~1,878 trips/day) than ETS (~2,040 trips/day) for
# total demand. This means ARIMA's point forecasts are on average closer to the
# actual daily counts during the test window. Given that daily totals fluctuate
# around 5,000-8,000 trips in December, an RMSE of roughly 1,900 represents an
# error of approximately 25-35% of the typical value - acceptable for a model
# trained on seasonal data that was not exposed to the full holiday dip.

# Cluster-level - RMSE
acc_cluster <- accuracy(fc_all_cluster, test_cluster) |>
  select(cluster, .model, RMSE, MAPE)

acc_cluster |> arrange(cluster, .model)

acc_cluster |>
  ggplot(aes(x = cluster, y = RMSE, fill = .model)) +
  geom_col(position = "dodge") +
  scale_fill_manual(values = c("ARIMA" = "#3399FF", "ETS" = "#FFBB33")) +
  labs(
    title = "RMSE by Cluster and Model",
    x = "Cluster", y = "RMSE (trips/day)", fill = "Model"
  ) +
  theme_bw(base_size = 13)

# Comment:
# RMSE scales with cluster volume: C1 has the highest absolute error (ARIMA:
# ~1,004; ETS: ~1,080) because it handles the most trips, while C4 has the lowest
# (ARIMA: ~104; ETS: ~123). In every cluster, ARIMA produces a lower RMSE than
# ETS, consistent with the total-demand result. The absolute errors also make sense
# as proportions - C3 and C4 errors are small in absolute terms even though those
# clusters have weaker seasonal structure.

# 4.2 MAPE

# Total demand - MAPE
acc_total |> select(.model, MAPE)

# Comment:
# MAPE values for total demand are high (~51% for ARIMA, ~54% for ETS). This is
# expected: MAPE penalises errors as a percentage of the actual value, and during
# the Christmas/holiday week actual trip counts are very low (sometimes below
# 3,000/day). Small absolute errors on those days translate to large percentage
# errors, inflating the MAPE beyond what the RMSE alone would suggest. ARIMA still
# outperforms ETS on this metric.

# Cluster-level - MAPE
acc_cluster |>
  select(cluster, .model, MAPE) |>
  arrange(cluster, .model)

acc_cluster |>
  ggplot(aes(x = cluster, y = MAPE, fill = .model)) +
  geom_col(position = "dodge") +
  scale_fill_manual(values = c("ARIMA" = "#3399FF", "ETS" = "#FFBB33")) +
  labs(
    title = "MAPE by Cluster and Model",
    x = "Cluster", y = "MAPE (%)", fill = "Model"
  ) +
  theme_bw(base_size = 13)

# Comment:
# Cluster-level MAPE ranges from ~39% (C4 ARIMA) to ~59% (C1 ETS). C4 - the
# lowest-volume cluster - achieves the best MAPE under ARIMA, suggesting that even
# with a simple seasonal model the relative accuracy is better when counts don't
# collapse as sharply during the holiday period. C3 is the one case where ETS beats
# ARIMA on MAPE (42% vs 47%), which aligns with its simpler ETS(A,N,N)
# specification adapting more conservatively to the low-volume series.

# Summary metrics table
metrics_all <- bind_rows(
  acc_total |> mutate(level = "Total", cluster = NA_character_),
  acc_cluster |> mutate(level = "Cluster")
) |>
  select(level, cluster, model = .model, RMSE, MAPE) |>
  arrange(level, cluster, model)

metrics_all

# Comment:
# The summary table consolidates all RMSE and MAPE values across both levels and
# both models. Across the board, ARIMA outperforms ETS on RMSE at every level, and
# on MAPE in all but one case (C3). The single ETS advantage in C3 MAPE is
# marginal and does not overturn the overall finding.

# 5. Conclusion

# Model Selection
metrics_all |>
  group_by(model) |>
  summarise(mean_RMSE = mean(RMSE), mean_MAPE = mean(MAPE))

# Comment:
# Averaging across all five series (Total + 4 clusters), ARIMA achieves a lower
# mean RMSE and mean MAPE than ETS. For this dataset - characterised by a strong
# downward trend entering the holiday period - ARIMA's differencing step gives it a
# meaningful advantage by allowing the forecast level to continue drifting downward
# rather than anchoring near the last observed training value.

# Key Findings:
#
# Seasonal structure: Both models correctly identify the weekly (period-7) seasonal
# pattern, which is the dominant short-term cycle in bike-share demand.
#
# Trend during test window: December 2019 sees an accelerated drop in demand due to
# the holiday period. ARIMA adapts to this through its integrated (differenced)
# component; ETS, without a trend term, produces a flatter and systematically high
# forecast during this window.
#
# Cluster heterogeneity: The four clusters differ not only in volume but in
# volatility. Higher-volume clusters (C1, C2) are better served by seasonal ARIMA;
# the lowest-volume cluster (C3) shows a case where simpler ETS smoothing matches
# or beats ARIMA on relative accuracy.
#
# Metric inflation: MAPE values are inflated by the very low trip counts on
# Christmas and adjacent days, where even small absolute errors produce large
# percentage errors. RMSE offers a more stable basis for comparison in this context.
#
# Overall recommendation: For operational demand forecasting of Divvy bike-share
# trips - particularly across seasonal transitions and holiday windows - ARIMA with
# weekly seasonality is the preferred model, providing lower absolute and relative
# errors across both the total network and individual station clusters.
