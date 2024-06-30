library(nanoparquet)
library(gha)

prev <- readRDS("to_collapse.rds")

paths <- Sys.glob('data/*.parquet')
gha_notice("{length(paths)} total paths")
dates <- paths |> basename() |> tools::file_path_sans_ext()
years <- format(as.Date(dates, '%Y-%m-%d'), '%Y')
to_collapse <- split(paths, years)

same <- vapply(
  names(to_collapse),
  \(year) setequal(prev[[year]], to_collapse[[year]]),
  logical(1)
)

for (i in seq_along(to_collapse)[!same]) {
  paths <- to_collapse[[i]]
  year <- names(to_collapse)[[i]]
  out_path <- sprintf('collapsed/%s.parquet', year)
  gha_notice("Updating {year} with {length(paths)} files")

  data <- paths |> lapply(read_parquet)
  data <- do.call(rbind, data)
  
  write_parquet(data, out_path)
}

saveRDS(to_collapse, 'to_collapse.rds')
