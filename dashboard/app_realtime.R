library(shiny)
library(bslib)
library(bsicons)
library(DBI)
library(RPostgres)
library(dplyr)
library(ggplot2)
library(plotly)
library(leaflet)
library(scales)
library(lubridate)

# Database connection details
PG_HOST <- Sys.getenv("POSTGRES_HOST", "localhost")
PG_PORT <- Sys.getenv("POSTGRES_PORT", "5432")
PG_USER <- Sys.getenv("POSTGRES_USER", "bikestream")
PG_PASS <- Sys.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB   <- Sys.getenv("POSTGRES_DB", "bikestream")

# Helper to get DB connection
get_db_conn <- function() {
  dbConnect(RPostgres::Postgres(),
            host = PG_HOST,
            port = PG_PORT,
            user = PG_USER,
            password = PG_PASS,
            dbname = PG_DB,
            bigint = "numeric")
}

ui <- page_navbar(
  title = "BikeStream Real-Time",
  theme = bs_theme(version = 5, bootswatch = "darkly", primary = "#00d4ff"),
  header = tags$head(
    tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
  ),
  
  nav_panel("Live Map",
    card(
      full_screen = TRUE,
      leafletOutput("live_map", height = "calc(100vh - 120px)")
    )
  ),
  
  nav_panel("System KPIs",
    layout_columns(
      value_box(
        title = "Total Active Stations",
        value = textOutput("kpi_stations"),
        showcase = bs_icon("diagram-3")
      ),
      value_box(
        title = "Total Bikes Available",
        value = textOutput("kpi_bikes"),
        showcase = bs_icon("bicycle")
      ),
      value_box(
        title = "Total Docks Available",
        value = textOutput("kpi_docks"),
        showcase = bs_icon("p-square")
      ),
      value_box(
        title = "Needs Rebalancing",
        value = textOutput("kpi_alerts"),
        showcase = bs_icon("exclamation-triangle"),
        theme = value_box_theme("danger")
      )
    ),
    card(
      card_header("City Overview"),
      tableOutput("city_summary")
    )
  ),
  
  nav_panel("Alerts (Rebalancing)",
    card(
      card_header("Stations needing immediate rebalancing (Empty/Full)"),
      tableOutput("alert_table")
    )
  ),
  
  nav_panel("Trends",
    layout_columns(
      col_widths = c(12, 12),
      card(
        card_header("System Health Composition (Last 12 Hours)"),
        plotlyOutput("health_trend_plot", height = "350px")
      ),
      card(
        card_header("City Utilization Heatmap (Last 12 Hours)"),
        plotlyOutput("heatmap_plot", height = "350px")
      )
    )
  )
)

server <- function(input, output, session) {
  # Refresh every 30 seconds
  autoInvalidate <- reactiveTimer(30000)
  
  # Reactive Data: Live Stations
  live_stations_data <- reactive({
    autoInvalidate()
    conn <- get_db_conn()
    on.exit(dbDisconnect(conn))
    
    query <- "
      SELECT DISTINCT ON (city, station_id)
          city, station_id, station_name, lat, lon,
          num_bikes_available, num_docks_available, num_ebikes_available,
          fill_ratio, status, capacity, time
      FROM station_snapshots
      ORDER BY city, station_id, time DESC;
    "
    dbGetQuery(conn, query)
  })
  
  # Reactive Data: KPIs
  kpi_data <- reactive({
    autoInvalidate()
    conn <- get_db_conn()
    on.exit(dbDisconnect(conn))
    
    query <- "
      SELECT city,
          SUM(num_bikes_available) as total_bikes,
          SUM(num_docks_available) as total_docks,
          SUM(num_ebikes_available) as total_ebikes,
          COUNT(*) as total_stations,
          SUM(CASE WHEN needs_rebalancing THEN 1 ELSE 0 END) as alert_count,
          AVG(fill_ratio) as avg_utilization
      FROM station_snapshots
      WHERE time > NOW() - INTERVAL '1 minute'
      GROUP BY city;
    "
    res <- dbGetQuery(conn, query)
    if(nrow(res) == 0) {
      data.frame(total_stations=0, total_bikes=0, total_docks=0, alert_count=0)
    } else {
      res
    }
  })
  
  # Map Output
  output$live_map <- renderLeaflet({
    data <- live_stations_data()
    if(nrow(data) == 0) return(leaflet() %>% addTiles())
    
    pal <- colorFactor(
      palette = c("red", "orange", "green", "blue"),
      domain = c("EMPTY", "LOW", "HEALTHY", "FULL")
    )
    
    leaflet(data) %>%
      addProviderTiles(providers$CartoDB.DarkMatter) %>%
      addCircleMarkers(
        ~lon, ~lat,
        color = ~pal(status),
        radius = 4,
        stroke = FALSE,
        fillOpacity = 0.8,
        popup = ~paste0(
          "<b>", station_name, "</b> (", city, ")<br>",
          "Status: ", status, "<br>",
          "Bikes: ", num_bikes_available, "<br>",
          "Docks: ", num_docks_available
        )
      ) %>%
      addLegend("bottomright", pal = pal, values = ~status, title = "Station Status")
  })
  
  # KPI Outputs
  output$kpi_stations <- renderText({ sum(kpi_data()$total_stations) })
  output$kpi_bikes <- renderText({ sum(kpi_data()$total_bikes) })
  output$kpi_docks <- renderText({ sum(kpi_data()$total_docks) })
  output$kpi_alerts <- renderText({ sum(kpi_data()$alert_count) })
  
  output$city_summary <- renderTable({
    kpi_data() %>% 
      mutate(avg_utilization = percent(avg_utilization, accuracy=0.1)) %>%
      select(City=city, Stations=total_stations, Bikes=total_bikes, Docks=total_docks, Alerts=alert_count, Utilization=avg_utilization)
  })
  
  # Alerts Output
  output$alert_table <- renderTable({
    autoInvalidate()
    conn <- get_db_conn()
    on.exit(dbDisconnect(conn))
    
    query <- "
      SELECT city as City, station_name as Station, status as Status,
          num_bikes_available as Bikes, num_docks_available as Docks, capacity as Capacity,
          time as Last_Seen
      FROM station_snapshots
      WHERE needs_rebalancing = TRUE
        AND time > NOW() - INTERVAL '5 minutes'
      ORDER BY time DESC
      LIMIT 100;
    "
    dbGetQuery(conn, query) %>%
      mutate(last_seen = as.character(last_seen))
  })
  
  # Health Trend Plot
  output$health_trend_plot <- renderPlotly({
    autoInvalidate()
    conn <- get_db_conn()
    on.exit(dbDisconnect(conn))
    
    query <- "
      SELECT time_bucket('15 minutes', time) AS bucket,
             status,
             COUNT(*) as count
      FROM station_snapshots
      WHERE time > NOW() - INTERVAL '12 hours'
        AND status IS NOT NULL
      GROUP BY bucket, status
      ORDER BY bucket, status;
    "
    health_data <- dbGetQuery(conn, query)
    
    if(nrow(health_data) == 0) return(plotly_empty())
    
    health_data$status <- factor(health_data$status, levels = c("EMPTY", "LOW", "HEALTHY", "FULL", "UNKNOWN"))
    
    p1 <- ggplot(health_data, aes(x = bucket, y = count, fill = status)) +
      geom_area(stat = "identity", position = "fill", alpha = 0.85) +
      scale_fill_manual(values = c("EMPTY" = "#dc3545", "LOW" = "#fd7e14", "HEALTHY" = "#20c997", "FULL" = "#0d6efd", "UNKNOWN" = "#6c757d"), na.value = "#6c757d") +
      scale_y_continuous(labels = scales::percent) +
      theme_minimal() +
      labs(x = NULL, y = "% of Stations", fill = "") +
      theme(
        plot.background = element_rect(fill = "transparent", color = NA),
        panel.background = element_rect(fill = "transparent", color = NA),
        text = element_text(color = "white"),
        axis.text = element_text(color = "gray"),
        legend.position = "bottom"
      )
    
    ggplotly(p1) %>%
      layout(legend = list(orientation = "h", x = 0.5, xanchor = "center", y = -0.15))
  })
  
  # Heatmap Plot
  output$heatmap_plot <- renderPlotly({
    autoInvalidate()
    conn <- get_db_conn()
    on.exit(dbDisconnect(conn))
    
    query <- "
      SELECT time_bucket('15 minutes', time) AS bucket,
          city,
          AVG(fill_ratio) AS avg_fill
      FROM station_snapshots
      WHERE time > NOW() - INTERVAL '12 hours'
      GROUP BY bucket, city
      ORDER BY bucket, city;
    "
    heatmap_data <- dbGetQuery(conn, query)
    
    if(nrow(heatmap_data) == 0) return(plotly_empty())
    
    plot_ly(
      data = heatmap_data,
      x = ~bucket,
      y = ~city,
      z = ~avg_fill,
      type = "heatmap",
      colors = colorRamp(c("#000004", "#51127c", "#b63679", "#fb8861", "#fcffa4")),
      colorbar = list(title = "Utilization", tickformat = ".0%")
    ) %>%
      layout(
        xaxis = list(title = "", gridcolor = "transparent", zeroline = FALSE, tickfont = list(color = "gray")),
        yaxis = list(title = "", gridcolor = "transparent", zeroline = FALSE, tickfont = list(color = "gray")),
        paper_bgcolor = "transparent",
        plot_bgcolor = "transparent",
        font = list(color = "white")
      )
  })
}

shinyApp(ui, server)
