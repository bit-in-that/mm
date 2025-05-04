box::use(
  R/a1_modules/af_pipelines
)

box::use(
  arrow[read_parquet],
  here[here],
  dplyr[...],
  dotenv[load_dot_env]
)

load_dot_env() # a .env file doesn't exist, create one with your session ID in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ids <- read_parquet(here("data/exports/2025/af_team_ids.parquet"))
af_team_ranks <- read_parquet(here("data/exports/2025/af_team_ranks.parquet"))
af_team_value <- read_parquet(here("data/exports/2025/af_team_value.parquet"))

players <- af_pipelines$players()

af_team_value |>
  filter(
    team_id == 3227
  ) |>
  View()

# TODO: try to rebuld this from the group up (not adding up to the number on the rankings page?)

3227 |>
  af_pipelines$team_lineups(session_id = session_id) |>
  left_join(players |> select("player_id", "cost", "first_name", "last_name"), by = "player_id") |> View()
  pull(cost) |>
  sum()

af_team_ids$name
af_team_ids$firstname
af_team_ids$lastname

af_team_value |>
  left_join(af_team_ids, by = "team_id") |>
  left_join(
    af_team_ranks |> filter(round == 2L) |> select(-round),
    by = c("team_id")
    ) |>
  group_by(round) |>
  mutate(
    value_rank = rank(-team_value) |> as.integer(),
  ) |>
  ungroup() |>
  filter(round == 3L) |>
  View()
