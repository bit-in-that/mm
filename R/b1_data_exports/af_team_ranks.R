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

af_team_ids <- arrow::read_parquet(here("data/exports/2025", "af_team_ids.parquet"))

af_team_ranks <- af_team_ids |>
  pull(user_id) |>
  af_pipelines$team_ranks_by_round(session_id = session_id) |>
  group_by(team_id) |>
  arrange(team_id, round) |>
  transmute(
    team_id,
    round,
    round_score = score,
    overall_score = cumsum(score),
    round_rank,
    overall_rank = rank
  ) |>
  ungroup()

write_parquet(af_team_ranks, here("data/exports/2025", "af_team_ranks.parquet"))
