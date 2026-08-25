# SETUP & LIBRARIES
# Core Libraries
library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(tibble)
library(forcats)
library(arrow)
library(tidygraph)
library(ggraph)
library(igraph)
library(reactable)
library(lubridate)

# Paths
base_dir <- "."
proc_dir <- file.path(base_dir, "dataset", "processed")
sta_dir <- file.path(base_dir, "dataset", "stations")
fig_dir <- file.path(base_dir, "figures", "network")
dir.create(fig_dir, showWarnings = FALSE, recursive = TRUE)

# Mathematical Framework & Data Summary
# 1. Function Definitions
get_quarterly_metrics <- function(data, q) {
    edges <- data %>%
        filter(quarter == q) %>%
        group_by(from_station_id, to_station_id) %>%
        summarise(weight = n(), .groups = "drop") %>%
        mutate(
            from = as.character(from_station_id),
            to = as.character(to_station_id)
        ) %>%
        select(from, to, weight) %>%
        drop_na()

    g_data <- as_tbl_graph(edges, directed = TRUE) %>%
        activate(nodes) %>%
        mutate(
            station_id = as.integer(name),
            degree = centrality_degree(mode = "all"),
            betweenness = centrality_betweenness(weights = weight),
            quarter = q
        ) %>%
        as_tibble() %>%
        left_join(stations, by = "station_id") %>%
        filter(!is.na(station_name)) %>%
        select(station_name, degree, betweenness, quarter)

    return(g_data)
}

# 2. Data Ingestion
read_divvy_quarterly <- function(q_label) {
    f <- file.path(proc_dir, paste0("divvy_trips_2019_", q_label, "_clean.parquet"))
    if (file.exists(f)) {
        read_parquet(f) %>%
            mutate(
                from_station_id = as.integer(from_station_id),
                to_station_id = as.integer(to_station_id),
                quarter = q_label
            )
    } else {
        warning("File not found: ", f)
        NULL
    }
}

all_trips <- bind_rows(
    read_divvy_quarterly("Q2"),
    read_divvy_quarterly("Q3"),
    read_divvy_quarterly("Q4")
)
stations <- read_csv(file.path(sta_dir, "stations.csv"), show_col_types = FALSE) %>%
    distinct(station_id, .keep_all = TRUE)

cat("Total trips loaded:", nrow(all_trips), "\n")

# 3. Summary Table
summary_table <- all_trips %>%
    group_by(quarter) %>%
    summarise(Total_Trips = n(), Unique_Routes = n_distinct(paste(from_station_id, to_station_id)))

reactable(summary_table, columns = list(Total_Trips = colDef(format = colFormat(separators = TRUE))))

# Time-Series Centrality Analysis
comparison_df <- bind_rows(
    get_quarterly_metrics(all_trips, "Q2"),
    get_quarterly_metrics(all_trips, "Q3"),
    get_quarterly_metrics(all_trips, "Q4")
)

# Plotting the Top 10 Stations' Betweenness evolution
top_stations <- comparison_df %>%
    group_by(station_name) %>%
    summarise(avg_b = mean(betweenness)) %>%
    arrange(desc(avg_b)) %>%
    head(10)

p_betweenness <- ggplot(
    comparison_df %>% filter(station_name %in% top_stations$station_name),
    aes(x = quarter, y = betweenness, group = station_name, color = station_name)
) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 3) +
    theme_minimal() +
    labs(
        title = "Evolution of Station 'Gatekeepers' (Betweenness)",
        subtitle = "How strategic importance shifts across 2019 quarters",
        x = "Quarter",
        y = "Betweenness Centrality Score"
    )

ggsave(file.path(fig_dir, "betweenness_evolution.png"), p_betweenness,
    width = 12, height = 6, dpi = 150
)

# Data preparation
final_edges <- all_trips %>%
    group_by(from_station_id, to_station_id) %>%
    summarise(weight = n(), .groups = "drop") %>%
    mutate(
        from = as.character(from_station_id),
        to = as.character(to_station_id)
    ) %>%
    select(from, to, weight) %>%
    filter(weight > 50)

# Graph construction
bike_graph <- as_tbl_graph(final_edges, directed = TRUE) %>%
    activate(nodes) %>%
    mutate(
        station_id = as.integer(name),
        degree = centrality_degree(mode = "all"),
        community = as.factor(group_infomap())
    ) %>%
    filter(!node_is_isolated()) %>%
    left_join(stations, by = c("station_id" = "station_id"))

# Visualization
p_network <- ggraph(bike_graph, layout = "lgl") +
    geom_edge_arc(aes(edge_width = weight, color = as.factor(from)),
        alpha = 0.2, show.legend = FALSE, strength = 0.1
    ) +
    geom_node_point(aes(size = degree, color = community), alpha = 0.8) +
    geom_node_text(aes(label = ifelse(degree > quantile(degree, 0.99), station_name, "")),
        repel = TRUE, size = 3, fontface = "bold", color = "black"
    ) +

    scale_edge_width(range = c(0.2, 1.5)) +
    scale_size_continuous(range = c(1, 6)) +

    theme_void() +
    coord_fixed(clip = "off") +

    labs(
        title = "Core Backbone of Chicago Divvy Network (2019)",
        subtitle = "Visualization filtered to show only major routes (>50 trips)",
        caption = "Method: LGL Layout | Noise Filter: Applied"
    )

ggsave(file.path(fig_dir, "network_backbone.png"), p_network,
    width = 14, height = 10, dpi = 150
)

cat("\nNetwork analysis complete. Figures saved to:", fig_dir, "\n")
