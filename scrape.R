library(clock)
library(gha)
library(nanoparquet)
library(httr2)
library(rvest)
library(stringr)

start <- date_build(2024, 1, 1)
dates <- date_seq(start, to = date_today("America/Chicago") - 1, by = 1)
# Only on weekdays
dates <- dates[as_weekday(dates) %in% c("Mon", "Tue", "Wed", "Thu", "Fri")]

holidays <- c("2024-01-01", "2024-05-17")
done <- tools::file_path_sans_ext(dir("data"))
todo <- dates[!dates %in% c(done, holidays)]
gha_notice("{length(todo)} dates to process")

base_url <- "https://www.houstonhealth.org/services/pollen-mold/houston-pollen-mold-count-"

find_html <- function(date) {
  gha_notice("Looking for {date}", title = "Downloading")
  urls <- paste0(
    tolower(date_format(date, format = "%A")), "-",
    tolower(date_format(date, format = "%B")), "-",
    get_day(date), c("", "-"), 
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
      httr2_http_404 = function(cnd) {}
    )
  }
  gha_notice("Failed", title = "Downloading")
  NULL
}

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
