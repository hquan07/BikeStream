library(utils)
options(timeout = max(1000, getOption("timeout")))

base_dir <- "."

raw_dir <- file.path(base_dir, "dataset", "raw")
dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)

urls <- c(
  "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
  "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
  "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
  "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2017_Q3Q4.zip"
)

cat("Starting automated download of Divvy raw data to:", raw_dir, "\n")
cat("Note: This might take a few minutes as files are large (~100-300MB each).\n")

for (url in urls) {
  zip_name <- basename(url)
  zip_path <- file.path(raw_dir, zip_name)
  
  if (!file.exists(zip_path)) {
    cat(sprintf("Downloading %s ...\n", zip_name))
    download.file(url, destfile = zip_path, mode = "wb", quiet = TRUE)
  } else {
    cat(sprintf("%s already exists. Skipping download.\n", zip_name))
  }
  
  cat(sprintf("Extracting %s ...\n", zip_name))
  # Unzip to a temporary directory
  tmp <- tempdir()
  unzip(zip_path, exdir = tmp)
  
  # Find CSV files
  csv_files <- list.files(tmp, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  
  # Copy necessary CSVs to raw_dir
  for (csv in csv_files) {
    base_csv <- basename(csv)
    # We only need the Trips and Stations files matching our patterns
    if (grepl("Divvy_Trips_2019", base_csv) || grepl("Divvy_Stations_2017_Q3Q4", base_csv)) {
      file.copy(csv, file.path(raw_dir, base_csv), overwrite = TRUE)
      cat(sprintf("  Moved %s to raw directory.\n", base_csv))
    }
  }
  
  # Cleanup temp files
  unlink(csv_files)
}

cat("\nVerification: List of files in dataset/raw:\n")
print(list.files(raw_dir))
cat("\nDownload and extraction complete!\n")
