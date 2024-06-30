library(nanoparquet)
library(tidyverse)

paths <- Sys.glob("data/*.parquet")
df <- paths |> map(nanoparquet::read_parquet) |> list_rbind()
df |> count(date)
df |> count(type)

thresholds <- read_csv("thresholds.csv")
# Check that we got all the species
df |> anti_join(categories, by = "type")

categories <- read_csv("categories.csv")

summaries <- df |> 
  left_join(types, by = "type") |> 
  summarise(count = sum(count, na.rm = TRUE), .by = c(category, date)) |> 
  left_join(thresholds, join_by(category, between(count, low, high))) |> 
  select(date, category, count, value)

summaries |> 
  ggplot(aes(date, count, colour = value)) +
  facet_wrap(~category, scales = "free_y", ncol = 1) +
  geom_point(aes(group = category))
