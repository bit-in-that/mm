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

# source from weekly cba's (Used for time on ground, weekly score, cba's and Ki )

# process
# 1. run Ownership numbers af
# 2. run Ownership numbers sc
# 3. run cba's
# 4. run change without map
# 5. run this script
# 6. re-run change script with map

# copy these into a _sent file

# load in template data
template_data <- read_csv(here('data','received','2025_R2_AF_inseason_player.csv'))
names <- names(template_data)

# load in session id
load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

# gather the data
current_round <- af_pipelines$current_round()
players <- af_pipelines$players()
squads <- af_pipelines$squads()
af_players_by_round <- af_pipelines$players_by_round()
player_stats <- fetch_player_stats_afl(season = 2025, round_number = current_round)

magic_number <- 9771
ppm_scaling_factor <- (0.329/0.393)

### data wrangling to get the weekly/overall price changes ###
af_players_by_curr_round <- af_players_by_round |>
  filter(round == current_round+1) |>
  select(player_id, price) |>
  rename(curr_price = price)

af_players_by_prev_round <- af_players_by_round |>
  filter(round == current_round) |>
  select(player_id, price) |>
  rename(prev_price = price)

af_players_price_change <- af_players_by_curr_round |>
  left_join(
    af_players_by_prev_round,
    by = "player_id"
  ) |>
  mutate(price_change = curr_price - prev_price) |>
  select("player_id", "price_change")

######################## FIX FIX FIX FIX FIX #########################

mm_cba <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("cba_r_",current_round,"_final.csv")))

mm_merge <- mm_cba |>
  mutate(playerId = as.integer(gsub("CD_I", "", playerId))) |>
  select("playerId", "TOG", "AF", "CBA_PERC", "KI_PERC", "CBA_PERC_chg", "KI_PERC_chg") |>
  rename("player_id" = "playerId",
         "CBAchg" = "CBA_PERC_chg",
         "KIchg" = "KI_PERC_chg")

## read in more mapping tables

seasonplayer_stats <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("season_avg_cba_ki_r_",current_round,".csv")))
seasonplayer_stats_l3 <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("l3_avg_cba_ki_r_",current_round,".csv")))
results_avg_win <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("results_avg_win_r_",current_round,".csv")))
results_avg_loss <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("results_avg_loss_r_",current_round,".csv")))
results_avg_home <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("results_avg_home_r_",current_round,".csv")))
results_avg_away <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("results_avg_away_r_",current_round,".csv")))

seasonplayer_stats <- seasonplayer_stats |>
  mutate(player_id = as.integer(gsub("CD_I", "", player_id))) |>
  rename("SeasCBA" = "season_cba",
         "SeasKI" = "season_ki") |>
  select(-c("season_tog"))

seasonplayer_stats_l3 <- seasonplayer_stats_l3 |>
  mutate(player_id = as.integer(gsub("CD_I", "", player_id))) |>
  rename("Last3CBA" = "season_cba",
         "Last3KI" = "season_ki",
         "Last3TG" = "season_tog")

ownership_numbers <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round),paste0("af_ownership_r_",current_round,"_final.csv")))

oppo_avg <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_r_",current_round,".csv")))
oppo_avg_l3 <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_l3_r_",current_round,".csv")))
oppo_avg_l1 <- read_csv(here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_l1_r_",current_round,".csv")))

oppo_avg <- oppo_avg |>
  mutate(player_id = as.integer(gsub("CD_I", "", player_id))) |>
  rename("OppCareer" = "oppo_avg")

oppo_avg_l3 <- oppo_avg_l3 |>
  mutate(player_id = as.integer(gsub("CD_I", "", player_id))) |>
  rename("OppLast3Score" = "oppo_avg")

oppo_avg_l1 <- oppo_avg_l1 |>
  mutate(player_id = as.integer(gsub("CD_I", "", player_id))) |>
  rename("OppLastScore" = "oppo_avg")

# merge everything onto players
players_exp <- players |>
  mutate(Player = paste0(first_name," ",last_name))|>
  left_join(
    squads |> select(squad_id, Team = short_name),
    by = "squad_id"
  ) |>
  left_join(
    af_players_price_change |> select(player_id, price_change),
    by = "player_id"
  ) |>
  left_join(
    mm_merge,
    by = "player_id"
  ) |>
  mutate(LastPPM = (AF/TOG)*ppm_scaling_factor) |>
  mutate(SeasAvg = total_points/games_played) |>
  rename(SeasTG = tog) |>
  mutate(SeasPPM = (SeasAvg/SeasTG)*ppm_scaling_factor) |>
  rename("Price" = "cost",
         "BreakEven" = "break_even") |>
  mutate(PricedAt = Price/magic_number) |>
  mutate(Position = toupper(position)) |>
  left_join(
    seasonplayer_stats,
    by = "player_id"
  ) |>
  left_join(
    seasonplayer_stats_l3,
    by = "player_id"
  ) |>
  mutate(Last3Score = last_3_avg) |>
  mutate(Last3PPM = (Last3Score/Last3TG)*ppm_scaling_factor) |>
  left_join(
    ownership_numbers,
    by = c("Player", "Team")
  ) |>
  left_join(
    results_avg_win, by = "player_id"
  ) |>
  left_join(
    results_avg_loss, by = "player_id"
  ) |>
  left_join(
    results_avg_home, by = "player_id"
  ) |>
  left_join(
    results_avg_away, by = "player_id"
  ) |>
  left_join(
    oppo_avg, by = "player_id"
  ) |>
  left_join(
    oppo_avg_l3, by = "player_id"
  ) |>
  left_join(
    oppo_avg_l1, by = "player_id"
  ) |>
  mutate(SeasTG = if_else(SeasAvg == 0, 0, SeasTG)) |>
  mutate(ReservesAvg = 0) |>
  mutate(ReservesLast = 0) |>
  select(Player, Team, Position, Price, PriceChg = price_change,
         PricedAt, BreakEven, LastScore = AF, LastTG = TOG, LastPPM,
         LastCBA = CBA_PERC, CBAchg, LastKI = KI_PERC, KIchg,
         SeasAvg, SeasTG, SeasPPM, SeasCBA, SeasKI,
         Last3Score, Last3TG, Last3PPM, Last3CBA, Last3KI,
         OwnershipTotal, OwnershipTop1000, OwnershipTop100,
         OwnershipTop10, OwnershipTop1000Chg, OwnershipTop100Chg,
         OwnershipTop10Chg, WinAvg, LossAvg, HomeAvg, AwayAvg,
         OppLastScore, OppLast3Score, OppCareer, ReservesAvg,
         ReservesLast) |>
  mutate(across(everything(), ~replace_na(., 0)))

check <- all(names(players_exp) == names)

fwrite(players_exp, here("data","exports","2025","_for_mm",paste0("b_round_0", current_round), paste0("2025_r_",current_round,"_AF_inseason_player.csv")))

