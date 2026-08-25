# File: dashboard/Dockerfile
FROM rocker/shiny:latest

# Install system dependencies for RPostgres and others
RUN apt-get update && apt-get install -y \
    libpq-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install required R packages
RUN R -e "install.packages(c('bslib', 'bsicons', 'DBI', 'RPostgres', 'dplyr', 'ggplot2', 'plotly', 'leaflet', 'scales', 'lubridate'), repos='https://cloud.r-project.org/')"

# Copy app and assets
WORKDIR /srv/shiny-server/
COPY app_realtime.R /srv/shiny-server/app.R
COPY www /srv/shiny-server/www

# Expose port
EXPOSE 3838

# Run
CMD ["/usr/bin/shiny-server"]
