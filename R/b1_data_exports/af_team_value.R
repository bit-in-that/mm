box::use(
  ../a1_modules/af_pipelines
)

box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  arrow[write_parquet, read_parquet]
)


load_dot_env() # a .env file doesn't exist, create one with your session ID in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ranks <- arrow::read_parquet(here("data/exports/2025", "af_team_ranks.parquet"))

rankings <- af_pipelines$rankings(session_id)

af_team_value <- af_team_ranks |>
  filter(round == max(round)) |>
  filter(overall_rank <= 100000) |>
  pull(team_id) |>
  af_pipelines$team_value(session_id = session_id)

write_parquet(af_team_value, here("data/exports/2025", "af_team_value.parquet"))
