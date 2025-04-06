## MT Code to replicate the AF MM Player Stats Table

box::use(
  ../a1_modules/af_pipelines
)
box::use(
  dotenv[load_dot_env],
  fitzRoy[fetch_player_stats_afl],
  here[here],
  readr[read_csv],
  dplyr[...],
  tidyr[replace_na],
  purrr[...],
  data.table[fwrite],
  jsonlite[write_json],
  arrow[write_parquet, read_parquet]
)

# load in template data
template_data <- read_csv(here('data','received','2025_R2_AF_inseason_player.csv'))
names <- names(template_data)


# # load in session id
# load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
# session_id <- Sys.getenv("AF_SESSION_ID")
# access_token <- Sys.getenv("SC_ACCESS_TOKEN")
#
# # gather the data
# current_round <- af_pipelines$current_round()
current_round <- 3
# players <- af_pipelines$players()


players <- readRDS(here("data","exports","MT_plane_downloads", paste0("players.rds")))
squads <- readRDS(here("data","exports","MT_plane_downloads", paste0("squads.rds")))

base_structure <- players |>
  mutate(Player = paste0(first_name," ",last_name)) |>
  left_join(
    squads |> select(squad_id, Team = short_name),
    by = "squad_id"
  ) |>
  mutate(Position = toupper(position)) |>
  mutate(LastTG = if_else(is.na(rd_tog), 0, rd_tog)) |>
  select(player_id, Player, Team, Position, LastTG,
         SeasTG = tog)

# then merge on these
# LastCBA	CBAchg	LastKI	KIchg
# SeasCBA	SeasKI	Last3Score	Last3TG	Last3PPM	Last3CBA	Last3KI

