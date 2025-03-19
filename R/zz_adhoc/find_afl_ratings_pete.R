box::use(
  dotenv[load_dot_env],
  arrow[read_parquet],
  dplyr[...],
  purrr[...],
  stringr[str_detect, regex],
  here[here]
)

box::use(
  ../a1_modules/af_pipelines
)


load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ids <- arrow::read_parquet(here("data/exports/2025/af_team_ids.parquet"))

players <- af_pipelines$players()

players |>
  transmute(player_id, player_name = paste(first_name, last_name)) |>
  View()

petes <- af_team_ids |>
  filter(str_detect(firstname, regex("^Pet", ignore_case = TRUE)))

pete_teams <- petes |>
  pull(team_id) |>
  af_pipelines$team_lineups(session_id = session_id)

pete_teams |>
  group_by(team_id, round, line_name) |>
  mutate(
    tail(player_id, 1),
    is_utility = (utility_position == line_name) & (player_id == tail(player_id, 1))
  ) |>
  ungroup()

pete_teams |>
  group_by(team_id) |>
  summarise(
    has_smith = 1006130 %in% player_id,
    has_rozee = 1001299 %in% player_id,
    has_trac = 298210 %in% player_id,
    .groups = "drop"
  ) |>
  mutate(
    might_be_pete = !has_smith & !has_rozee & has_trac
  ) |>
  filter(might_be_pete) |>
  pull(team_id) |>
  unique()



# possibles:
# - 1319
#

