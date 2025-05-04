box::use(
  dotenv[load_dot_env],
  arrow[read_parquet],
  dplyr[...],
  purrr[...],
  here[here],
  data.table[fread],
  tidyr[...],
  stringr[...]
)

box::use(
  ../a1_modules/af_pipelines
)


load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

players <- af_pipelines$players()
squads <- af_pipelines$squads()

af_team_ids <- read_parquet(here("data/exports/2025", "af_team_ids.parquet"))
af_coach_list_initial <- fread(here("R/a0_inputs", "af_coach_list.csv")) |>
  as_tibble()

af_coach_list <- af_coach_list_initial |>
  filter(include) |>
  left_join(af_team_ids, by = "user_id")

af_coach_list_ranks <- af_coach_list |>
  pull(user_id) |>
  af_pipelines$team_ranks(session_id = session_id)

af_coach_list_team_lineups <- af_coach_list |>
  pull(team_id) |>
  af_pipelines$team_lineups(session_id = session_id)

af_coach_list_teams <- af_coach_list |>
  pull(team_id) |>
  af_pipelines$teams(session_id = session_id)


af_coach_list_team_lineups |>
  full_join(
    players |> transmute(player_id, Player = paste(first_name, last_name), OwnershipTotal = owned_by, squad_id),
    by = "player_id"
  ) |>
  left_join(
    squads |> select(squad_id, Team = short_name),
    by = "squad_id"
  ) |>
  group_by(
    player_id, Player, Team, OwnershipTotal
  ) |>
  summarise(
    OwnershipHighProfile = sum(!is.na(team_id)) / nrow(af_coach_list) * 100,
    .groups = "drop"
  ) |>
  left_join(
    fread(here("data/exports/2025/_for_mm", paste0("af_ownership_r1.csv"))) |>
      as_tibble() |>
      select(Player, OwnershipTop1000, OwnershipTop100, OwnershipTop10),
    by = "Player"
  ) |>
  mutate(
    diff = OwnershipTop1000 - OwnershipHighProfile
  ) |>
  View()

af_coach_list_team_lineups |>
  left_join(af_coach_list_ranks, by = "team_id") |>
  group_by(team_id, firstname, lastname, name) |>
  summarise(
    Has_Sam_Taylor = 1005247 %in% player_id,
    Pivotable = sum(line_name == "Def") == 9,
    .groups = "drop"
    ) |>
  filter(Has_Sam_Taylor) |>
  View()

af_coach_list_team_lineups |>
  left_join(af_coach_list_ranks, by = "team_id") |>
  group_by(team_id, firstname, lastname, name) |>
  summarise(
    Has_Lindsay = 1028531L %in% player_id,
    Has_Knevit = 1021103 %in% player_id,
    .groups = "drop"
    ) |>
  View()


af_coach_list_team_lineups |>
  left_join(players, by = "player_id") |>
  View()


af_coach_list_team_lineups |>
  filter(team_id == 22L) |>
  left_join(players, by = "player_id") |>
  mutate(
    col_name = if_else(is_bench, "Bench", line_name),
    extra_text = case_when(
      is_bench ~ paste0(" (", str_sub(line_name, 1, 1), ")"),
      is_captain ~ " (C)",
      is_vice_captain ~ " (VC)",
      TRUE ~ ""
    ),
    player_text = paste0(first_name, " ", last_name, extra_text)
  ) |>
  select(col_name, player_text) |>
  group_by(col_name) |>
  mutate(`#` = row_number()) |>
  pivot_wider(names_from = col_name, values_from = player_text) |>
  reactable::reactable()

