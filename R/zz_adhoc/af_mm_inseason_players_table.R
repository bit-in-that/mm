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


# load in session id
load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")


# gather the data
current_round <- af_pipelines$current_round()
players <- af_pipelines$players()
squads <- af_pipelines$squads()
af_players_by_round <- af_pipelines$players_by_round()
player_stats <- fetch_player_stats_afl(season = 2025, round_number = current_round)
previous_round <- current_round-1

magic_number <- 9771


### data wrangling to get the weekly/overall price changes ###
af_players_by_curr_round <- af_players_by_round |>
  filter(round == current_round) |>
  select(player_id, price) |>
  rename(curr_price = price)

af_players_by_prev_round <- af_players_by_round |>
  filter(round == previous_round) |>
  select(player_id, price) |>
  rename(prev_price = price)

af_players_price_change <- af_players_by_curr_round |>
  left_join(
    af_players_by_prev_round,
    by = "player_id"
  ) |>
  mutate(price_change = curr_price - prev_price) |>
  select("player_id", "price_change")


# source from weekly cba's (Used for time on ground, weekly score, cba's and Ki )
mm_cba <- read_csv(here("data","exports","2025","_for_mm",paste0("round_",previous_round),paste0("cba_r",previous_round,".csv")))
mm_cba_prev <- read_csv(here("data","exports","2025","_for_mm",paste0("round_",previous_round-1),paste0("cba_r",previous_round-1,".csv")))

mm_merge <- mm_cba |>
  mutate(playerId = as.integer(gsub("CD_I", "", playerId))) |>
  select("playerId", "TOG", "AF", "CBA_PERC", "KI_PERC") |>
  rename("player_id" = "playerId")

mm_merge_prev <- mm_cba_prev |>
  mutate(playerId = as.integer(gsub("CD_I", "", playerId))) |>
  select("playerId", "CBA_PERC", "KI_PERC") |>
  rename("player_id" = "playerId",
         "CBA_PERC_1" = "CBA_PERC",
         "KI_PERC_1" = "KI_PERC")


diff_with_na <- function(x, y) {
  if(is.na(x) && is.na(y)) return(0)
  if(is.na(x)) return(-y)
  if(is.na(y)) return(x)
  return(x - y)
}

mm_merge |>
  left_join(
    mm_merge_prev,
    by = "player_id"
  ) |>
  rowwise() |>
  mutate(CBAchg = diff_with_na(CBA_PERC, CBA_PERC_1)) |>
  mutate(KIchg = diff_with_na(KI_PERC, KI_PERC_1)) |>
  ungroup() |>
  select("player_id", "TOG", "AF", "CBA_PERC", "KI_PERC",
         "CBAchg", "KIchg")

## read in more mapping tables

seasonplayer_stats <- read_csv(here("data","exports","2025","_for_mm","season",paste0("season_avg_cba_ki_r",current_round,".csv")))
seasonplayer_stats_l3 <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("season_avg_cba_ki_r",current_round,".csv")))
results_avg_win <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("results_avg_win",current_round,".csv")))
results_avg_loss <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("results_avg_loss",current_round,".csv")))
results_avg_home <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("results_avg_home",current_round,".csv")))
results_avg_away <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("results_avg_away",current_round,".csv")))

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


ownership_numbers <- read_csv(here("data","exports","2025","_for_mm",paste0("af_ownership_r",current_round-1,"_final.csv")))


oppo_avg <- read_csv(here("data","exports","2025","_for_mm","season",paste0("oppo_avg_r",current_round,".csv")))
oppo_avg_l3 <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("oppo_avg_l3_r",current_round,".csv")))
oppo_avg_l1 <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("oppo_avg_l1_r",current_round,".csv")))

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
  left_join(
    mm_merge_prev,
    by = "player_id"
  ) |>
  mutate(LastPPM = AF/TOG) |>
  mutate(SeasAvg = total_points/games_played) |>
  rename(SeasTG = tog) |>
  mutate(SeasPPM = SeasAvg/SeasTG) |>
  rowwise() |>
  mutate(CBAchg = diff_with_na(CBA_PERC, CBA_PERC_1)) |>
  mutate(KIchg = diff_with_na(KI_PERC, KI_PERC_1)) |>
  ungroup() |>
  select(-c("CBA_PERC_1", "KI_PERC_1")) |>
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
  mutate(Last3PPM = Last3Score/Last3TG) |>
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

fwrite(players_exp, here("data","exports","2025","_for_mm",paste0("2025_R",current_round-1,"_AF_inseason_player.csv")))

