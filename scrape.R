library(clock)
library(gha)
library(nanoparquet)
library(httr2)
library(rvest)
library(stringr)

# Figure out which days to download --------------------------------------------
# Implemented this way so that we automatically "backfill" the data; i.e. if
# a script fails to run one day, the missing day of data will automatically be 
# filled in the next time the script runs. The minor downside of this technique 
# is that because the pollen data isn't collected on local government holidays
# we'll retry multiple urls until we notice and record in `holidays`

start <- date_today("America/Chicago") - 7 # date_build(2023, 8, 16)
dates <- date_seq(start, to = date_today("America/Chicago") - 1, by = 1)
# Only on weekdays
dates <- dates[as_weekday(dates) %in% c("Mon", "Tue", "Wed", "Thu", "Fri")]

holidays <- c("2024-01-01", "2024-05-17")
done <- tools::file_path_sans_ext(dir("data"))
todo <- dates[!dates %in% c(done, holidays)]
gha_notice("{length(todo)} dates to process")

paste_grid <- function(...) {
  do.call(paste0, rev(do.call(expand.grid, rev(list(...)))))
}

# Find the data ----------------------------------------------------------------
# It looks like the URLS are human created because they appear to randomly
# vary between a few formats. We just try each one in turn, carefully logging
# to make sure its obvious what happened.

base_url <- "https://www.houstonhealth.org/services/pollen-mold/"
find_html <- function(date) {
  gha_notice("Looking for {date}", title = "Downloading")
  urls <- paste_grid(
    c("houston-pollen-mold-count-", "pollen-mold-"),
    tolower(date_format(date, format = "%A")), "-",
    tolower(date_format(date, format = "%B")), "-",
    get_day(date), c("-", ""), 
    get_year(date)
  )

  for (url in urls) {
    gha_notice("Trying {url}", title = "Downloading")
    tryCatch(
      {
        resp <- req_perform(request(paste0(base_url, url)))
        gha_notice("Found it!", title = "Downloading")
        return(resp_body_html(resp))
      },

      # Only catch the specific case of a file not found because we want other 
      # errors to bubble up and cause the action to fail and signal us
      httr2_http_404 = function(cnd) {}
    )
  }
  gha_notice("Failed", title = "Downloading")
  NULL
}

# Parse and save it ------------------------------------------------------------

for (date in as.list(todo)) {
  html <- find_html(date[[1]])
  if (is.null(html)) next

  li <- html_nodes(html, ".coh-style-multi-column-two-column li")
  gha_notice("Found {length(li)} bullets", title = "Parsing")

  pieces <- str_split_fixed(html_text(li), ":\\s*", 2)
  df <- data.frame(
    date = date,
    type = pieces[, 1],
    count = as.integer(str_replace(pieces[, 2], ",", ""))
  )
  path <- file.path("data", paste0(date, ".parquet"))
  gha_notice("Writing {path}", title = "Parsing")
  nanoparquet::write_parquet(df, path)
}

gha_notice("Done! 🚀")
