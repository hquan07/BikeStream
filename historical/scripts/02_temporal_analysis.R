# 1. Setup
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
library(scales)
library(viridis)
library(patchwork) # combine plots
library(ggrepel) # label repelling

# Colour palette (consistent across plots)
PALETTE_SEQ <- "YlOrRd" # sequential  (heatmap)
CLRS <- c("#2C7BB6", "#D7191C", "#1A9641") # subscriber / customer / total

# Output directories
base_dir <- "."
fig_dir <- file.path(base_dir, "figures", "temporal")
dir.create(fig_dir, showWarnings = FALSE, recursive = TRUE)

proc_dir <- file.path(base_dir, "dataset", "processed")

# 2. Load & Combine Cleaned Quarters
cat("Loading cleaned data ...\n")

load_quarter <- function(quarter_label) {
    parquet_file <- file.path(proc_dir, paste0("divvy_trips_2019_", quarter_label, "_clean.parquet"))
    
    if (file.exists(parquet_file)) {
        cat("  Reading Parquet:", basename(parquet_file), "\n")
        df <- read_parquet(parquet_file)
    } else {
        stop(
            "Parquet file not found for quarter ", quarter_label,
            "\nLooked for:\n  ", parquet_file,
            "\nRun 01_merge_clean.R first."
        )
    }

    df %>% mutate(quarter = quarter_label)
}

trips <- bind_rows(
    load_quarter("Q2"),
    load_quarter("Q3"),
    load_quarter("Q4")
)

cat("Total rows loaded:", nrow(trips), "\n")

# 3. Feature Engineering
cat("Engineering time features ...\n")

trips <- trips %>%
    mutate(
        # time components from start_time
        hour = hour(start_time),
        wday = wday(start_time, label = TRUE, abbr = TRUE, week_start = 1),
        wday_num = wday(start_time, week_start = 1), # 1=Mon … 7=Sun
        month = month(start_time, label = TRUE, abbr = TRUE),
        week_num = isoweek(start_time), # ISO week of year
        date = as_date(start_time),
        is_weekend = wday_num >= 6,
        # user type simplified
        user_type = case_when(
            str_to_lower(usertype) == "subscriber" ~ "Subscriber",
            str_to_lower(usertype) == "customer" ~ "Customer",
            TRUE ~ "Other"
        )
    )

# 4. Hourly Pattern Analysis
cat("--- Hourly Pattern ---\n")

hourly <- trips %>%
    count(hour, user_type) %>%
    group_by(hour) %>%
    mutate(total = sum(n)) %>%
    ungroup()

hourly_total <- trips %>%
    count(hour, name = "n")

# Plot 4a: Ride count by hour (stacked by user type)
p_hourly <- ggplot(hourly, aes(x = hour, y = n, fill = user_type)) +
    geom_col(position = "stack", width = 0.8) +
    scale_fill_manual(values = c("Subscriber" = "#2C7BB6", "Customer" = "#D7191C", "Other" = "#888888")) +
    scale_x_continuous(
        breaks = 0:23,
        labels = sprintf("%02d:00", 0:23)
    ) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title    = "Hourly Ride Distribution",
        subtitle = "Divvy Bike-Sharing · Chicago · 2019 (Q2–Q4)",
        x        = "Hour of Day",
        y        = "Number of Rides",
        fill     = "User Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(
        axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
        panel.grid.minor = element_blank(),
        legend.position = "top"
    )

ggsave(file.path(fig_dir, "hourly_distribution.png"),
    p_hourly,
    width = 14, height = 6, dpi = 150
)

# Plot 4b: Weekday vs. Weekend hourly profile
hourly_we <- trips %>%
    count(hour, is_weekend) %>%
    mutate(day_type = if_else(is_weekend, "Weekend", "Weekday"))

p_hourly_we <- ggplot(hourly_we, aes(x = hour, y = n, colour = day_type)) +
    geom_line(size = 1.2) +
    geom_point(size = 2.5) +
    scale_colour_manual(values = c("Weekday" = "#2C7BB6", "Weekend" = "#D7191C")) +
    scale_x_continuous(breaks = seq(0, 23, 3)) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title   = "Hourly Profile: Weekday vs. Weekend",
        x       = "Hour of Day",
        y       = "Number of Rides",
        colour  = NULL
    ) +
    theme_minimal(base_size = 13) +
    theme(
        panel.grid.minor = element_blank(),
        legend.position  = "top"
    )

ggsave(file.path(fig_dir, "hourly_weekday_vs_weekend.png"),
    p_hourly_we,
    width = 10, height = 6, dpi = 150
)

# 5. Daily Pattern Analysis
cat("--- Daily Pattern ---\n")

daily <- trips %>%
    count(date, is_weekend) %>%
    mutate(day_type = if_else(is_weekend, "Weekend", "Weekday"))

daily_stats <- daily %>%
    group_by(day_type) %>%
    summarise(
        mean_rides   = mean(n),
        median_rides = median(n),
        sd_rides     = sd(n),
        .groups      = "drop"
    )

cat("\nDaily statistics:\n")
print(daily_stats)

# Plot 5a: Daily ride count time-series
p_daily_ts <- ggplot(daily, aes(x = date, y = n)) +
    geom_col(aes(fill = day_type), width = 1) +
    geom_smooth(colour = "black", se = FALSE, method = "loess", span = 0.15, linewidth = 0.8) +
    scale_fill_manual(values = c("Weekday" = "#4393C3", "Weekend" = "#D6604D")) +
    scale_x_date(date_breaks = "1 month", date_labels = "%b") +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title   = "Daily Ride Count (2019 Q2–Q4)",
        x       = NULL,
        y       = "Rides per Day",
        fill    = "Day Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(
        panel.grid.minor = element_blank(),
        legend.position  = "top"
    )

ggsave(file.path(fig_dir, "daily_timeseries.png"),
    p_daily_ts,
    width = 14, height = 5, dpi = 150
)

# Plot 5b: Distribution of daily counts
p_daily_dist <- ggplot(daily, aes(x = n, fill = day_type)) +
    geom_histogram(binwidth = 500, colour = "white", alpha = 0.85) +
    scale_fill_manual(values = c("Weekday" = "#4393C3", "Weekend" = "#D6604D")) +
    scale_x_continuous(labels = label_comma()) +
    labs(
        title = "Distribution of Daily Ride Counts",
        x     = "Rides per Day",
        y     = "Number of Days",
        fill  = "Day Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank(), legend.position = "top")

ggsave(file.path(fig_dir, "daily_distribution_hist.png"),
    p_daily_dist,
    width = 10, height = 5, dpi = 150
)

# 6. Weekly Pattern Analysis
cat("--- Weekly Pattern ---\n")

# 6a: By day-of-week
weekday_summary <- trips %>%
    count(wday, wday_num, user_type) %>%
    arrange(wday_num)

p_weekday <- ggplot(weekday_summary, aes(x = wday, y = n, fill = user_type)) +
    geom_col(position = "dodge", width = 0.7) +
    scale_fill_manual(values = c("Subscriber" = "#2C7BB6", "Customer" = "#D7191C", "Other" = "#888888")) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title  = "Rides by Day of Week",
        x      = NULL,
        y      = "Total Rides",
        fill   = "User Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank(), legend.position = "top")

ggsave(file.path(fig_dir, "weekly_by_weekday.png"),
    p_weekday,
    width = 10, height = 5, dpi = 150
)

# 6b: Weekly total trend (ISO week aggregation)
weekly_total <- trips %>%
    count(week_num, month) %>%
    # keep only common week numbers (avoid cross-year artefacts)
    filter(week_num >= 14, week_num <= 52)

p_weekly_trend <- ggplot(weekly_total, aes(x = week_num, y = n, fill = month)) +
    geom_col(width = 0.9) +
    scale_fill_viridis_d(option = "C", begin = 0.15, end = 0.9) +
    scale_x_continuous(
        breaks = seq(14, 52, 4),
        labels = function(x) paste("Wk", x)
    ) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title  = "Weekly Ride Volume Trend",
        x      = "ISO Week",
        y      = "Total Rides",
        fill   = "Month"
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank(), legend.position = "top")

ggsave(file.path(fig_dir, "weekly_trend.png"),
    p_weekly_trend,
    width = 14, height = 5, dpi = 150
)

# 7. Peak Hour Detection
cat("--- Peak Hour Detection ---\n")

# Compute hourly totals and rank for both weekday and weekend
hourly_we_full <- trips %>%
    count(is_weekend, hour) %>%
    mutate(day_type = if_else(is_weekend, "Weekend", "Weekday"))

peak_hours <- hourly_we_full %>%
    group_by(day_type) %>%
    arrange(desc(n)) %>%
    mutate(rank = row_number()) %>%
    ungroup()

top_peak <- peak_hours %>% filter(rank <= 3)

cat("\nTop 3 peak hours (Weekday):\n")
print(top_peak %>% filter(day_type == "Weekday") %>% select(hour, n))

cat("\nTop 3 peak hours (Weekend):\n")
print(top_peak %>% filter(day_type == "Weekend") %>% select(hour, n))

# Classify hours into sessions
classify_session <- function(h) {
    case_when(
        h >= 5 & h < 9 ~ "Morning Rush (05-09)",
        h >= 9 & h < 12 ~ "Mid-Morning (09-12)",
        h >= 12 & h < 14 ~ "Lunch (12-14)",
        h >= 14 & h < 17 ~ "Afternoon (14-17)",
        h >= 17 & h < 20 ~ "Evening Rush (17-20)",
        h >= 20 | h < 5 ~ "Night (20-05)",
        TRUE ~ "Unknown"
    )
}

session_summary <- trips %>%
    mutate(session = classify_session(hour)) %>%
    count(session, user_type) %>%
    mutate(session = factor(session, levels = c(
        "Morning Rush (05-09)", "Mid-Morning (09-12)", "Lunch (12-14)",
        "Afternoon (14-17)", "Evening Rush (17-20)", "Night (20-05)"
    )))

p_session <- ggplot(session_summary, aes(x = session, y = n, fill = user_type)) +
    geom_col(position = "stack") +
    scale_fill_manual(values = c("Subscriber" = "#2C7BB6", "Customer" = "#D7191C", "Other" = "#888888")) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title  = "Rides by Time-of-Day Session",
        x      = NULL,
        y      = "Total Rides",
        fill   = "User Type"
    ) +
    theme_minimal(base_size = 13) +
    theme(
        axis.text.x = element_text(angle = 20, hjust = 1),
        panel.grid.minor = element_blank(),
        legend.position = "top"
    )

ggsave(file.path(fig_dir, "peak_hour_sessions.png"),
    p_session,
    width = 12, height = 6, dpi = 150
)

# Highlight peaks on hourly profile
p_peak_highlight <- ggplot(hourly_we_full, aes(x = hour, y = n, group = day_type)) +
    geom_area(aes(fill = day_type), alpha = 0.25, position = "identity") +
    geom_line(aes(colour = day_type), size = 1.1) +
    geom_point(
        data = top_peak, aes(colour = day_type), size = 4, shape = 21,
        fill = "gold", stroke = 1.5
    ) +
    geom_label_repel(
        data = top_peak,
        aes(label = sprintf("%02d:00\n%s rides", hour, label_comma()(n))),
        size = 3, box.padding = 0.5, max.overlaps = 20
    ) +
    scale_fill_manual(values = c("Weekday" = "#2C7BB6", "Weekend" = "#D7191C")) +
    scale_colour_manual(values = c("Weekday" = "#2C7BB6", "Weekend" = "#D7191C")) +
    scale_x_continuous(
        breaks = seq(0, 23, 3),
        labels = sprintf("%02d:00", seq(0, 23, 3))
    ) +
    scale_y_continuous(labels = label_comma()) +
    labs(
        title = "Peak Hours Highlighted (Top 3 per Day Type)",
        subtitle = "Gold points = top-3 peak hours",
        x = "Hour of Day",
        y = "Number of Rides",
        fill = NULL, colour = NULL
    ) +
    theme_minimal(base_size = 13) +
    theme(panel.grid.minor = element_blank(), legend.position = "top")

ggsave(file.path(fig_dir, "peak_hours_highlighted.png"),
    p_peak_highlight,
    width = 14, height = 6, dpi = 150
)

# 8. Time-Based Heatmap (Hour × Day-of-Week)
cat("--- Time-Based Heatmap ---\n")

heatmap_data <- trips %>%
    count(wday, wday_num, hour) %>%
    arrange(wday_num)

# 8a: Overall heatmap
p_heatmap <- ggplot(heatmap_data, aes(x = hour, y = wday, fill = n)) +
    geom_tile(colour = "white", linewidth = 0.4) +
    scale_fill_viridis_c(
        option  = "magma",
        begin   = 0.05,
        end     = 0.95,
        labels  = label_comma(),
        name    = "Rides"
    ) +
    scale_x_continuous(
        breaks = seq(0, 23, 1),
        labels = sprintf("%02d", 0:23),
        expand = c(0, 0)
    ) +
    scale_y_discrete(limits = rev) + # Mon at top, Sun at bottom
    labs(
        title    = "Heatmap: Ride Volume by Hour × Day of Week",
        subtitle = "Divvy Bike-Sharing · Chicago · 2019 (Q2–Q4)",
        x        = "Hour of Day",
        y        = NULL
    ) +
    theme_minimal(base_size = 13) +
    theme(
        axis.text.x      = element_text(size = 8),
        panel.grid       = element_blank(),
        legend.key.width = unit(1.5, "cm")
    )

ggsave(file.path(fig_dir, "heatmap_hour_weekday.png"),
    p_heatmap,
    width = 16, height = 5, dpi = 150
)

# 8b: Separate heatmaps for each user type
heatmap_user <- trips %>%
    filter(user_type != "Other") %>%
    count(user_type, wday, wday_num, hour) %>%
    arrange(wday_num)

p_heatmap_user <- ggplot(heatmap_user, aes(x = hour, y = wday, fill = n)) +
    geom_tile(colour = "white", linewidth = 0.4) +
    scale_fill_viridis_c(
        option  = "magma",
        begin   = 0.05,
        end     = 0.95,
        labels  = label_comma(),
        name    = "Rides"
    ) +
    scale_x_continuous(
        breaks = seq(0, 23, 3),
        labels = sprintf("%02d:00", seq(0, 23, 3)),
        expand = c(0, 0)
    ) +
    scale_y_discrete(limits = rev) +
    facet_wrap(~user_type, ncol = 1, scales = "free_y") +
    labs(
        title    = "Heatmap by User Type: Hour × Day of Week",
        x        = "Hour of Day",
        y        = NULL
    ) +
    theme_minimal(base_size = 12) +
    theme(
        axis.text.x      = element_text(angle = 30, hjust = 1, size = 8),
        panel.grid       = element_blank(),
        legend.key.width = unit(1.2, "cm"),
        strip.text       = element_text(face = "bold", size = 12)
    )

ggsave(file.path(fig_dir, "heatmap_hour_weekday_by_usertype.png"),
    p_heatmap_user,
    width = 14, height = 8, dpi = 150
)

# 8c: Monthly heatmap (Month × Hour) — ride volume across months
monthly_hourly <- trips %>%
    count(month, hour)

p_month_heatmap <- ggplot(monthly_hourly, aes(x = hour, y = month, fill = n)) +
    geom_tile(colour = "white", linewidth = 0.5) +
    scale_fill_viridis_c(
        option  = "viridis",
        begin   = 0.05,
        end     = 0.98,
        labels  = label_comma(),
        name    = "Rides"
    ) +
    scale_x_continuous(
        breaks = seq(0, 23, 1),
        labels = sprintf("%02d", 0:23),
        expand = c(0, 0)
    ) +
    scale_y_discrete(limits = rev) +
    labs(
        title    = "Heatmap: Ride Volume by Hour × Month",
        x        = "Hour of Day",
        y        = NULL
    ) +
    theme_minimal(base_size = 13) +
    theme(
        axis.text.x      = element_text(size = 8),
        panel.grid       = element_blank(),
        legend.key.width = unit(1.5, "cm")
    )

ggsave(file.path(fig_dir, "heatmap_hour_month.png"),
    p_month_heatmap,
    width = 16, height = 5, dpi = 150
)

# 9. Summary Table
cat("\n TEMPORAL PATTERN SUMMARY \n")

cat("\n-- Hourly: Top 5 Busiest Hours (Overall) --\n")
hourly_total %>%
    arrange(desc(n)) %>%
    slice_head(n = 5) %>%
    mutate(hour_label = sprintf("%02d:00 – %02d:00", hour, hour + 1)) %>%
    select(Hour = hour_label, Rides = n) %>%
    print()

cat("\n-- Daily: Average Rides per Day --\n")
daily_stats %>%
    mutate(across(where(is.numeric), ~ round(., 0))) %>%
    print()

cat("\n-- Weekly: Busiest Day of Week --\n")
trips %>%
    count(wday, wday_num) %>%
    arrange(desc(n)) %>%
    slice_head(n = 3) %>%
    select(Weekday = wday, Rides = n) %>%
    print()

cat("\n-- Session Breakdown --\n")
trips %>%
    mutate(session = classify_session(hour)) %>%
    count(session) %>%
    mutate(pct = round(100 * n / sum(n), 1)) %>%
    arrange(desc(n)) %>%
    print()

# Save summary tables
tables_dir <- file.path(base_dir, "tables")
dir.create(tables_dir, showWarnings = FALSE, recursive = TRUE)

write_csv(hourly_total, file.path(tables_dir, "hourly_totals.csv"))
write_csv(daily, file.path(tables_dir, "daily_rides.csv"))
write_csv(daily_stats, file.path(tables_dir, "daily_stats.csv"))
write_csv(heatmap_data, file.path(tables_dir, "heatmap_hour_weekday.csv"))

cat("\n DONE \n")
cat("Figures saved to:", fig_dir, "\n")
cat("Tables  saved to:", tables_dir, "\n")
