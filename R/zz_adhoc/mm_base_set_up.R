## MT Code to replicate the AF MM Player Stats Table
box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines,
  ../b1_data_exports/af_ownership_top1000,
  ../b1_data_exports/sc_ownership_top1000,
  ../b1_data_exports/sc_breakevens,
  ../b1_data_exports/weekly_cbas_ki,
  ../b1_data_exports/magic_number
)

box::use(
  here[here],
  dplyr[...],
  purrr[...],
  readxl[read_excel],
  data.table[fwrite]
)

# define season
season <- 2025

# data collection
current_round <- af_pipelines$current_round()
af_players <- af_pipelines$players()
squads <- af_pipelines$squads()

# fix this one up later
sc_breakevens <- sc_breakevens$sc_breakevens()
af_ownership <- af_ownership_top1000$af_own()
sc_ownership <- sc_ownership_top1000$sc_own()
mm_cba <- weekly_cbas_ki$cba_ki()
af_magic_number <- magic_number$af_magic_number(c_round = current_round)
sc_magic_number <- magic_number$sc_magic_number(c_round = current_round)
# sc_magic_number <- 5177.453

# adhoc changes (monitor throughout the season with Mid season draft)
adhoc_changes_id <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "player_id")
adhoc_changes_player <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "player")

# removing the trailing space
# changing nic martin's player id in the player_stats as it doesn't like up with the
af_players <- af_players |>
  mutate(player_id = paste0("CD_I", player_id)) |>
  left_join(adhoc_changes_player |>
              select(player_id, surname),
            by = "player_id") |>
  mutate(last_name = if_else(is.na(surname), last_name, surname)) |>
  left_join(adhoc_changes_id |>
              select(-Name),
            by = "player_id") |>
  mutate(player_id = if_else(is.na(player.playerId), player_id, player.playerId)) |>
  select(-c(player.playerId, surname))


# creating the base structure
base_structure <- af_players |>
  mutate(Player = paste0(first_name," ",last_name)) |>
  left_join(
    squads |> select(squad_id, Team = short_name),
    by = "squad_id"
  ) |>
  mutate(Position = toupper(position)) |>
  mutate(LastTG = if_else(is.na(rd_tog), 0, rd_tog)) |>
  mutate(Season = season) |>
  mutate(Round = current_round) |>
  select(Season, Round, player_id, Player,
         squad_id, Team, Position, af_cost = cost,
         af_be = break_even)

# improve this by updating the weekly_cba script and then just pull it in
# join everything on

base_plus <- base_structure |>
  left_join(mm_cba,
            by = "player_id") |>
  left_join(sc_breakevens,
            by = c("Player" = "player_name")) |>
  left_join(sc_ownership |> select(-c("feed_id", "Player", "Team")),
            by = "player_id") |>
  left_join(af_ownership |> select(-c("Player", "Team")),
            by = "player_id") |>
  mutate(
    af_magic_number = af_magic_number,
    sc_magic_number = sc_magic_number,
    af_priced_at = af_cost/af_magic_number,
    sc_priced_at = sc_cost/sc_magic_number) |>
  mutate(minutes_played = if_else(is.na(AF),NA,minutes_played),
         SC = if_else(is.na(AF),NA,SC)) |>
  mutate(Opposition = if_else(team.name == home.team.name,away.team.name,home.team.name)) |>
  mutate(home_status = if_else(team.name == home.team.name, "home", "away")) |>
  select(-c(away.team.name,home.team.name))

fwrite(base_plus, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("mm_master_table_r_",current_round,".csv")))
