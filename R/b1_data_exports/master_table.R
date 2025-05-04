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
  tidyr[pivot_wider, replace_na],
  fitzRoy[fetch_player_stats_afl, fetch_results_afl],
  purrr[...],
  stringr[str_pad, str_detect],
  readr[read_csv],
  data.table[fwrite],
  RMySQL[MySQL],
  DBI[dbConnect, dbWriteTable, dbExecute, dbGetQuery]
)

db_host <- "ls-7189a7c3f9e8e50019ede4ba0e86c98674eaf21a.czyw0iuiknog.ap-southeast-2.rds.amazonaws.com"
db_name <- "mm_data"
db_user <- "dbmasteruser"
db_password <- Sys.getenv("sql_password")
db_port <- 3306  # Default MySQL port

con <- dbConnect(
  RMySQL::MySQL(),
  host = db_host,
  dbname = db_name,
  user = db_user,
  password = db_password,
  port = db_port
)

# define season
season <- 2025
current_round <- af_pipelines$current_round()
# current_round <- 5

# read in previous
data_prev <- dbGetQuery(con, paste0("SELECT * FROM master_table"))
data_curr <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("mm_master_table_r_",current_round,".csv")))

data_prev <- data_prev |>
  select(-c("next_opp")) |>
  left_join(data_curr |> select(player_id, next_opp),
            by = "player_id")

data_prev <- data_prev |>
  select(all_of(names(data_curr)))

master_table <- rbind(data_curr, data_prev)

master_table <- master_table |>
  mutate(Opposition = if_else(Opposition == "Adelaide", "Adelaide Crows",
                              if_else(Opposition == "Gold Coast Suns", "Gold Coast SUNS", Opposition)))

current_season <- max(master_table$Season)
current_round <- max(master_table |>
                       filter(Season == current_season) |> pull(Round))

af_price_projector <- function(current_season, current_round){

  player_info <- master_table |>
    filter(Season == current_season, Round == current_round) |>
    select(Player, Team, Position, Price = af_cost, PricedAt = af_priced_at, Breakeven = af_be)

  # 2. Get yearly averages for 2023, 2024, 2025
  yearly_avg <- master_table |>
    filter(Season %in% c(2023, 2024, current_season)) |>
    group_by(Player, Season) |>
    summarise(SeasonAvg = mean(AF, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = Season, values_from = SeasonAvg, names_prefix = "Avg") |>
    rename(SeasonAvg = paste0("Avg", current_season))

  # 3. Get Last N Scores (L01 to L05)
  last_n_scores <- master_table |>
    filter(Season == current_season, !is.na(AF)) |>
    arrange(Player, desc(Round)) |>
    group_by(Player) |>
    mutate(row = row_number()) |>
    filter(row <= 5) |>
    select(Player, row, AF) |>
    pivot_wider(names_from = row, values_from = AF, names_prefix = "L") |>
    rename_with(~ paste0("L", str_pad(gsub("L", "", .), 2, pad = "0")), starts_with("L"))


  # manual change

  last_n_scores <- last_n_scores |>
    mutate(L05 = as.double(NA))


  # 4. Calculate Last3Avg and Last5Avg
  last_x_avg <- last_n_scores |>
    rowwise() |>
    mutate(
      Last3Avg = mean(c_across(c(L01, L02, L03))[c_across(c(L01, L02, L03)) > 0], na.rm = TRUE),
      Last5Avg = mean(c_across(c(L01, L02, L03, L04, L05))[c_across(c(L01, L02, L03, L04, L05)) > 0], na.rm = TRUE)
    ) |>
    ungroup()

  # 5. Combine everything
  final_table <- player_info |>
    left_join(yearly_avg, by = "Player") |>
    left_join(last_x_avg, by = "Player") |>
    mutate(
      LastScore = L01
    ) |>
    select(Player, Team, Position, Price, PricedAt, Breakeven,
           Avg2023 = Avg2023, Avg2024 = Avg2024,
           L01, L02, L03, L04, L05,
           LastScore, SeasonAvg, Last3Avg, Last5Avg)

  # Optional: sort for neatness
  final_table <- final_table |> arrange(Player) |>
    mutate(across(everything(), ~replace_na(., 0)))

  return(final_table)

}
sc_price_projector <- function(current_season, current_round){

  player_info <- master_table |>
    filter(Season == current_season, Round == current_round) |>
    select(Player, Team, Position, Price = sc_cost, PricedAt = sc_priced_at, Breakeven = sc_be)

  # 2. Get yearly averages for 2023, 2024, 2025
  yearly_avg <- master_table |>
    filter(Season %in% c(2023, 2024, current_season)) |>
    group_by(Player, Season) |>
    summarise(SeasonAvg = mean(SC, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = Season, values_from = SeasonAvg, names_prefix = "Avg") |>
    rename(SeasonAvg = paste0("Avg", current_season))

  # 3. Get Last N Scores (L01 to L05)
  last_n_scores <- master_table |>
    filter(Season == current_season, !is.na(SC)) |>
    arrange(Player, desc(Round)) |>
    group_by(Player) |>
    mutate(row = row_number()) |>
    filter(row <= 5) |>
    select(Player, row, SC) |>
    pivot_wider(names_from = row, values_from = SC, names_prefix = "L") |>
    rename_with(~ paste0("L", str_pad(gsub("L", "", .), 2, pad = "0")), starts_with("L"))


  # manual change

  last_n_scores <- last_n_scores |>
    mutate(L05 = as.double(NA))


  # 4. Calculate Last3Avg and Last5Avg
  last_x_avg <- last_n_scores |>
    rowwise() |>
    mutate(
      Last3Avg = mean(c_across(c(L01, L02, L03))[c_across(c(L01, L02, L03)) > 0], na.rm = TRUE),
      Last5Avg = mean(c_across(c(L01, L02, L03, L04, L05))[c_across(c(L01, L02, L03, L04, L05)) > 0], na.rm = TRUE)
    ) |>
    ungroup()

  # 5. Combine everything
  final_table <- player_info |>
    left_join(yearly_avg, by = "Player") |>
    left_join(last_x_avg, by = "Player") |>
    mutate(
      LastScore = L01
    ) |>
    select(Player, Team, Position, Price, PricedAt, Breakeven,
           Avg2023 = Avg2023, Avg2024 = Avg2024,
           L01, L02, L03, L04, L05,
           LastScore, SeasonAvg, Last3Avg, Last5Avg)

  # Optional: sort for neatness
  final_table <- final_table |> arrange(Player) |>
    mutate(across(everything(), ~replace_na(., 0)))

  return(final_table)

}
af_team_summary <- function(current_season){

  # 1. Filter the season data to only include the current season and valid data
  season_data <- master_table |> filter(Season == current_season)
  season_data <- season_data |> filter(!is.na(AF))

  # 2. Create position flags
  season_data <- season_data |>
    mutate(
      is_DEF = str_detect(Position, "DEF"),
      is_MID = str_detect(Position, "MID"),
      is_RUC = str_detect(Position, "RUC"),
      is_FOR = str_detect(Position, "FWD")
    )

  # --- Opponent team top performers: flip team.name and Opposition
  season_data_opp <- season_data |> mutate(team.name = Opposition)

  # 3. Create a distinct set of matches
  distinct_matches <- season_data |>
    distinct(Season, Round, team.name, matchId)

  # Order games by team and round (most recent = 1)
  game_ordered <- distinct_matches |>
    arrange(team.name, desc(Round)) |>
    group_by(team.name) |>
    mutate(game_number_by_team = row_number()) |>
    ungroup()

  # 4. Join game order info back to the main data
  season_data <- season_data |>
    left_join(game_ordered, by = c("Season", "Round", "team.name", "matchId"))

  # 3. Create a distinct set of matches
  distinct_matches_opp <- season_data_opp |>
    distinct(Season, Round, team.name, matchId)

  # Order games by team and round (most recent = 1)
  game_ordered_opp <- distinct_matches_opp |>
    arrange(team.name, desc(Round)) |>
    group_by(team.name) |>
    mutate(game_number_by_team = row_number()) |>
    ungroup()

  # 4. Join game order info back to the main data
  season_data_opp <- season_data_opp |>
    left_join(game_ordered_opp, by = c("Season", "Round", "team.name", "matchId"))

  # 5. Summary functions
  # --- Points by round ---
  sum_points_by_round <- season_data |>
    group_by(team.name, game_number_by_team) |>
    summarise(total_points = sum(AF, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = game_number_by_team, values_from = total_points, names_prefix = "game_")

  sum_points_by_round_opp <- season_data_opp |>
    group_by(team.name, game_number_by_team) |>
    summarise(total_points = sum(AF, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = game_number_by_team, values_from = total_points, names_prefix = "game_")

  # --- Season total ---
  season_total <- season_data |>
    group_by(team.name) |>
    summarise(Season = 23*mean(AF, na.rm = TRUE), .groups = "drop")

  season_total_opp <- season_data_opp |>
    group_by(team.name) |>
    summarise(SeasonOpp = 23*mean(AF, na.rm = TRUE), .groups = "drop")

  # --- Last 3 games points ---
  last3 <- season_data |>
    filter(game_number_by_team <= 3) |>
    group_by(team.name) |>
    summarise(Last3 = 23*mean(AF, na.rm = TRUE), .groups = "drop")

  last3_opp <- season_data_opp |>
    filter(game_number_by_team <= 3) |>
    group_by(team.name) |>
    summarise(Last3Opp = 23*mean(AF, na.rm = TRUE), .groups = "drop")


  # 6. Function to calculate average of top N scorers by position
  position_summary <- function(df, position_flag, top_n) {
    df |>
      filter(.data[[position_flag]]) |>
      group_by(team.name, matchId) |>
      arrange(team.name, matchId, desc(AF)) |>
      slice_head(n = top_n) |>
      group_by(team.name) |>
      summarise(mean = mean(AF, na.rm = TRUE), .groups = "drop")
  }

  # --- Own team top performers ---
  top3_mid <- position_summary(season_data, "is_MID", 3) |>
    rename(Top3Mid = mean)

  top_ruc <- position_summary(season_data, "is_RUC", 1) |>
    rename(TopRuc = mean)

  top2_def <- position_summary(season_data, "is_DEF", 2) |>
    rename(Top2Def = mean)

  top2_for <- position_summary(season_data, "is_FOR", 2) |>
    rename(Top2For = mean)

  top3_opp_mid <- position_summary(season_data_opp, "is_MID", 3) |>
    rename(Top3OppMid = mean)

  top_opp_ruc <- position_summary(season_data_opp, "is_RUC", 1) |>
    rename(TopOppRuc = mean)

  top2_opp_def <- position_summary(season_data_opp, "is_DEF", 2) |>
    rename(Top2OppDef = mean)

  top2_opp_for <- position_summary(season_data_opp, "is_FOR", 2) |>
    rename(Top2OppFor = mean)

  # 7. Create team abbreviation mapping
  team_abbreviations <- tibble(
    team.name = c("Adelaide Crows", "Brisbane Lions", "Carlton", "Collingwood", "Essendon", "Fremantle",
                  "GWS GIANTS", "Geelong Cats", "Gold Coast SUNS", "Hawthorn", "Melbourne", "North Melbourne",
                  "Port Adelaide", "Richmond", "St Kilda", "Sydney Swans", "West Coast Eagles", "Western Bulldogs"),
    Icon = c("ADE", "BRL", "CAR", "COLL", "ESS", "FRE", "GWS", "GEE", "GCS", "HAW", "MEL", "NTH",
             "PORT", "RICH", "STK", "SYD", "WCE", "WB")
  )

  # 8. Get distinct team names from season_data to ensure we have all teams
  all_teams <- season_data |>
    distinct(team.name) |>
    filter(!is.na(team.name))

  # 9. Assemble final table with proper formatting
  final_table <- all_teams |>
    left_join(team_abbreviations, by = "team.name") |>
    rename(Team = team.name) |>
    left_join(sum_points_by_round |> select(team.name, Last = game_1), by = c("Team" = "team.name")) |>
    left_join(sum_points_by_round_opp |> select(team.name, LastOpp = game_1), by = c("Team" = "team.name")) |>
    left_join(season_total, by = c("Team" = "team.name")) |>
    left_join(season_total_opp, by = c("Team" = "team.name")) |>
    left_join(last3, by = c("Team" = "team.name")) |>
    left_join(last3_opp, by = c("Team" = "team.name")) |>
    left_join(top3_mid, by = c("Team" = "team.name")) |>
    left_join(top3_opp_mid, by = c("Team" = "team.name")) |>
    left_join(top_ruc, by = c("Team" = "team.name")) |>
    left_join(top_opp_ruc, by = c("Team" = "team.name")) |>
    left_join(top2_def, by = c("Team" = "team.name")) |>
    left_join(top2_opp_def, by = c("Team" = "team.name")) |>
    left_join(top2_for, by = c("Team" = "team.name")) |>
    left_join(top2_opp_for, by = c("Team" = "team.name")) |>
    # Round numeric columns to integers for clean display
    mutate(across(c(Last, LastOpp, Season, SeasonOpp, Last3, Last3Opp), ~round(., 0))) |>
    mutate(across(c(Top3Mid, Top3OppMid, TopRuc, TopOppRuc, Top2Def, Top2OppDef, Top2For, Top2OppFor), ~round(., 0))) |>
    # Ensure proper column order
    select(Team, Icon, Last, LastOpp, Season, SeasonOpp, Last3, Last3Opp,
           Top3Mid, Top3OppMid, TopRuc, TopOppRuc, Top2Def, Top2OppDef, Top2For, Top2OppFor)

  # Display final table
  final_table <- final_table |> arrange(Team)

  return(final_table)


}
sc_team_summary <- function(current_season){

  # 1. Filter the season data to only include the current season and valid data
  season_data <- master_table |> filter(Season == current_season)
  season_data <- season_data |> filter(!is.na(SC))

  # 2. Create position flags
  season_data <- season_data |>
    mutate(
      is_DEF = str_detect(Position, "DEF"),
      is_MID = str_detect(Position, "MID"),
      is_RUC = str_detect(Position, "RUC"),
      is_FOR = str_detect(Position, "FWD")
    )

  # --- Opponent team top performers: flip team.name and Opposition
  season_data_opp <- season_data |> mutate(team.name = Opposition)

  # 3. Create a distinct set of matches
  distinct_matches <- season_data |>
    distinct(Season, Round, team.name, matchId)

  # Order games by team and round (most recent = 1)
  game_ordered <- distinct_matches |>
    arrange(team.name, desc(Round)) |>
    group_by(team.name) |>
    mutate(game_number_by_team = row_number()) |>
    ungroup()

  # 4. Join game order info back to the main data
  season_data <- season_data |>
    left_join(game_ordered, by = c("Season", "Round", "team.name", "matchId"))

  # 3. Create a distinct set of matches
  distinct_matches_opp <- season_data_opp |>
    distinct(Season, Round, team.name, matchId)

  # Order games by team and round (most recent = 1)
  game_ordered_opp <- distinct_matches_opp |>
    arrange(team.name, desc(Round)) |>
    group_by(team.name) |>
    mutate(game_number_by_team = row_number()) |>
    ungroup()

  # 4. Join game order info back to the main data
  season_data_opp <- season_data_opp |>
    left_join(game_ordered_opp, by = c("Season", "Round", "team.name", "matchId"))

  # 5. Summary functions
  # --- Points by round ---
  sum_points_by_round <- season_data |>
    group_by(team.name, game_number_by_team) |>
    summarise(total_points = sum(SC, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = game_number_by_team, values_from = total_points, names_prefix = "game_")

  sum_points_by_round_opp <- season_data_opp |>
    group_by(team.name, game_number_by_team) |>
    summarise(total_points = sum(SC, na.rm = TRUE), .groups = "drop") |>
    pivot_wider(names_from = game_number_by_team, values_from = total_points, names_prefix = "game_")

  # --- Season total ---
  season_total <- season_data |>
    group_by(team.name) |>
    summarise(Season = 23*mean(SC, na.rm = TRUE), .groups = "drop")

  season_total_opp <- season_data_opp |>
    group_by(team.name) |>
    summarise(SeasonOpp = 23*mean(SC, na.rm = TRUE), .groups = "drop")

  # --- Last 3 games points ---
  last3 <- season_data |>
    filter(game_number_by_team <= 3) |>
    group_by(team.name) |>
    summarise(Last3 = 23*mean(SC, na.rm = TRUE), .groups = "drop")

  last3_opp <- season_data_opp |>
    filter(game_number_by_team <= 3) |>
    group_by(team.name) |>
    summarise(Last3Opp = 23*mean(SC, na.rm = TRUE), .groups = "drop")


  # 6. Function to calculate average of top N scorers by position
  position_summary <- function(df, position_flag, top_n) {
    df |>
      filter(.data[[position_flag]]) |>
      group_by(team.name, matchId) |>
      arrange(team.name, matchId, desc(SC)) |>
      slice_head(n = top_n) |>
      group_by(team.name) |>
      summarise(mean = mean(SC, na.rm = TRUE), .groups = "drop")
  }

  # --- Own team top performers ---
  top3_mid <- position_summary(season_data, "is_MID", 3) |>
    rename(Top3Mid = mean)

  top_ruc <- position_summary(season_data, "is_RUC", 1) |>
    rename(TopRuc = mean)

  top2_def <- position_summary(season_data, "is_DEF", 2) |>
    rename(Top2Def = mean)

  top2_for <- position_summary(season_data, "is_FOR", 2) |>
    rename(Top2For = mean)

  top3_opp_mid <- position_summary(season_data_opp, "is_MID", 3) |>
    rename(Top3OppMid = mean)

  top_opp_ruc <- position_summary(season_data_opp, "is_RUC", 1) |>
    rename(TopOppRuc = mean)

  top2_opp_def <- position_summary(season_data_opp, "is_DEF", 2) |>
    rename(Top2OppDef = mean)

  top2_opp_for <- position_summary(season_data_opp, "is_FOR", 2) |>
    rename(Top2OppFor = mean)

  # 7. Create team abbreviation mapping
  team_abbreviations <- tibble(
    team.name = c("Adelaide Crows", "Brisbane Lions", "Carlton", "Collingwood", "Essendon", "Fremantle",
                  "GWS GIANTS", "Geelong Cats", "Gold Coast SUNS", "Hawthorn", "Melbourne", "North Melbourne",
                  "Port Adelaide", "Richmond", "St Kilda", "Sydney Swans", "West Coast Eagles", "Western Bulldogs"),
    Icon = c("ADE", "BRL", "CAR", "COLL", "ESS", "FRE", "GWS", "GEE", "GCS", "HAW", "MEL", "NTH",
             "PORT", "RICH", "STK", "SYD", "WCE", "WB")
  )

  # 8. Get distinct team names from season_data to ensure we have all teams
  all_teams <- season_data |>
    distinct(team.name) |>
    filter(!is.na(team.name))

  # 9. Assemble final table with proper formatting
  final_table <- all_teams |>
    left_join(team_abbreviations, by = "team.name") |>
    rename(Team = team.name) |>
    left_join(sum_points_by_round |> select(team.name, Last = game_1), by = c("Team" = "team.name")) |>
    left_join(sum_points_by_round_opp |> select(team.name, LastOpp = game_1), by = c("Team" = "team.name")) |>
    left_join(season_total, by = c("Team" = "team.name")) |>
    left_join(season_total_opp, by = c("Team" = "team.name")) |>
    left_join(last3, by = c("Team" = "team.name")) |>
    left_join(last3_opp, by = c("Team" = "team.name")) |>
    left_join(top3_mid, by = c("Team" = "team.name")) |>
    left_join(top3_opp_mid, by = c("Team" = "team.name")) |>
    left_join(top_ruc, by = c("Team" = "team.name")) |>
    left_join(top_opp_ruc, by = c("Team" = "team.name")) |>
    left_join(top2_def, by = c("Team" = "team.name")) |>
    left_join(top2_opp_def, by = c("Team" = "team.name")) |>
    left_join(top2_for, by = c("Team" = "team.name")) |>
    left_join(top2_opp_for, by = c("Team" = "team.name")) |>
    # Round numeric columns to integers for clean display
    mutate(across(c(Last, LastOpp, Season, SeasonOpp, Last3, Last3Opp), ~round(., 0))) |>
    mutate(across(c(Top3Mid, Top3OppMid, TopRuc, TopOppRuc, Top2Def, Top2OppDef, Top2For, Top2OppFor), ~round(., 0))) |>
    # Ensure proper column order
    select(Team, Icon, Last, LastOpp, Season, SeasonOpp, Last3, Last3Opp,
           Top3Mid, Top3OppMid, TopRuc, TopOppRuc, Top2Def, Top2OppDef, Top2For, Top2OppFor)

  # Display final table
  final_table <- final_table |> arrange(Team)

  return(final_table)


}

af_big_table <- function(current_season, current_round){

  season_data <- master_table |> filter(Season == current_season)
  season_data <- season_data |> filter(!is.na(AF))

  season_data <- season_data |>
    arrange(player_id, desc(Round)) |>
    group_by(player_id) |>
    mutate(row_num = row_number()) |>
    ungroup()

  last_3_data <- season_data |>
    filter(row_num <= 3)

  master_table <- master_table |>
    mutate(CBA_PERC = if_else(is.na(CBA_PERC),0,CBA_PERC)) |>
    mutate(KI_PERC = if_else(is.na(KI_PERC),0,KI_PERC))

  # Filter for current round data
  current_round_data <- master_table |>
    filter(Season == current_season, Round == current_round) |>
    select(player_id, Player, Team, Position,
           Price = af_cost, PricedAt = af_priced_at, BreakEven = af_be,
           LastScore = AF, LastTG = TOG, minutes_played,
           LastCBA = CBA_PERC, LastKI = KI_PERC,
           OwnershipTotal = af_OwnershipTotal,
           OwnershipTop1000 = af_OwnershipTop1000,
           OwnershipTop100 = af_OwnershipTop100,
           OwnershipTop10 = af_OwnershipTop10,
           Result = result)

  # 1. Price Change and Ownership Change
  previous_round_data <- master_table |>
    filter(Season == current_season, Round == current_round - 1) |>
    select(player_id,
           Prev_Price = af_cost,
           Prev_CBA = CBA_PERC,
           Prev_KI = KI_PERC,
           Prev_OwnershipTop1000 = af_OwnershipTop1000,
           Prev_OwnershipTop100 = af_OwnershipTop100,
           Prev_OwnershipTop10 = af_OwnershipTop10)

  starting_round_data <- master_table |>
    filter(Season == current_season, Round == 0) |>
    select(player_id,
           Starting_Price = af_cost)

  price_and_ownership_chg <- current_round_data |>
    left_join(previous_round_data, by = "player_id") |>
    left_join(starting_round_data, by = "player_id") |>
    mutate(
      PriceChg = Price - Prev_Price,
      SeasonPriceChange = Price - Starting_Price,
      CBAchg = LastCBA - Prev_CBA,
      KIchg = LastKI - Prev_KI,
      OwnershipTop1000Chg = OwnershipTop1000 - Prev_OwnershipTop1000,
      OwnershipTop100Chg = OwnershipTop100 - Prev_OwnershipTop100,
      OwnershipTop10Chg = OwnershipTop10 - Prev_OwnershipTop10
    ) |>
    select(player_id, SeasonPriceChange, PriceChg, CBAchg, KIchg, OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg)

  # 2. Last PPM
  current_round_data <- current_round_data |>
    mutate(LastPPM = LastScore / minutes_played)

  # 3. Season Averages

  season_avg <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF)) |>
    group_by(player_id) |>
    summarise(
      SeasAvg = mean(AF, na.rm = TRUE),
      SeasTG = mean(TOG, na.rm = TRUE),
      SeasPPM = mean(AF / minutes_played, na.rm = TRUE),
      SeasCBA = mean(CBA_PERC, na.rm = TRUE),
      SeasKI = mean(KI, na.rm = TRUE),
      .groups = "drop"
    )



  season_avg_win <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF)) |>
    filter(result == "W") |>
    group_by(player_id) |>
    summarise(
      WinAvg = mean(AF, na.rm = TRUE),
      .groups = "drop"
    )

  season_avg_loss <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF)) |>
    filter(result == "L") |>
    group_by(player_id) |>
    summarise(
      LossAvg = mean(AF, na.rm = TRUE),
      .groups = "drop"
    )


  season_avg_home <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF)) |>
    filter(home_status == "home") |>
    group_by(player_id) |>
    summarise(
      HomeAvg = mean(AF, na.rm = TRUE),
      .groups = "drop"
    )


  season_avg_away <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF)) |>
    filter(home_status == "away") |>
    group_by(player_id) |>
    summarise(
      AwayAvg = mean(AF, na.rm = TRUE),
      .groups = "drop"
    )



  # 4. Last 3 Game Averages
  last3 <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(AF), !is.na(minutes_played)) |>
    arrange(player_id, desc(Season), desc(Round)) |>
    group_by(player_id) |>
    slice_head(n = 3) |>
    summarise(
      Last3Score = mean(AF, na.rm = TRUE),
      Last3TG = mean(TOG, na.rm = TRUE),
      Last3CBA = mean(CBA_PERC, na.rm = TRUE),
      Last3KI = mean(KI_PERC, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(Last3PPM = Last3Score/Last3TG)

  opp_table <- master_table |>
    filter(Opposition == next_opp) |>
    arrange(player_id, desc(Season), desc(Round)) |>
    group_by(player_id) |>
    mutate(row = row_number()) |>
    ungroup()

  opp_1 <- opp_table |>
    filter(row == 1) |>
    group_by(player_id) |>
    summarise(OppLastScore = mean(AF))

  opp_3 <- opp_table |>
    filter(row <= 3) |>
    group_by(player_id) |>
    summarise(OppLast3Score = mean(AF))

  opp_all <- opp_table |>
    group_by(player_id) |>
    summarise(OppCareer = mean(AF))



  # 5. Final Join
  final_table <- current_round_data |>
    left_join(price_and_ownership_chg, by = "player_id") |>
    left_join(season_avg, by = "player_id") |>
    left_join(last3, by = "player_id") |>
    left_join(season_avg_loss, by = "player_id") |>
    left_join(season_avg_win, by = "player_id") |>
    left_join(season_avg_home, by = "player_id") |>
    left_join(season_avg_away, by = "player_id") |>
    left_join(opp_1, by = "player_id") |>
    left_join(opp_3, by = "player_id") |>
    left_join(opp_all, by = "player_id") |>
    mutate(
      ReservesAvg = 0,
      ReservesLast = 0
    ) |>
    select(Player, Team, Position, Price, SeasonPriceChange, PriceChg, PricedAt, BreakEven,
           LastScore, LastTG, LastPPM, LastCBA, CBAchg, LastKI, KIchg,
           SeasAvg, SeasTG, SeasPPM, SeasCBA, SeasKI,
           Last3Score, Last3TG, Last3PPM, Last3CBA, Last3KI,
           OwnershipTotal, OwnershipTop1000, OwnershipTop100, OwnershipTop10,
           OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg,
           WinAvg, LossAvg, HomeAvg, AwayAvg,
           OppLastScore, OppLast3Score, OppCareer, ReservesAvg, ReservesLast)

  return(final_table)



}
sc_big_table <- function(current_season, current_round){

  season_data <- master_table |> filter(Season == current_season)
  season_data <- season_data |> filter(!is.na(SC))

  season_data <- season_data |>
    arrange(player_id, desc(Round)) |>
    group_by(player_id) |>
    mutate(row_num = row_number()) |>
    ungroup()

  last_3_data <- season_data |>
    filter(row_num <= 3)

  master_table <- master_table |>
    mutate(CBA_PERC = if_else(is.na(CBA_PERC),0,CBA_PERC)) |>
    mutate(KI_PERC = if_else(is.na(KI_PERC),0,KI_PERC))

  # Filter for current round data
  current_round_data <- master_table |>
    filter(Season == current_season, Round == current_round) |>
    select(player_id, Player, Team, Position,
           Price = sc_cost, PricedAt = sc_priced_at, BreakEven = sc_be,
           LastScore = SC, LastTG = TOG, minutes_played,
           LastCBA = CBA_PERC, LastKI = KI_PERC,
           OwnershipTotal = sc_OwnershipTotal,
           OwnershipTop1000 = sc_OwnershipTop1000,
           OwnershipTop100 = sc_OwnershipTop100,
           OwnershipTop10 = sc_OwnershipTop10,
           Result = result)

  # 1. Price Change and Ownership Change
  previous_round_data <- master_table |>
    filter(Season == current_season, Round == current_round - 1) |>
    select(player_id,
           Prev_Price = sc_cost,
           Prev_CBA = CBA_PERC,
           Prev_KI = KI_PERC,
           Prev_OwnershipTop1000 = sc_OwnershipTop1000,
           Prev_OwnershipTop100 = sc_OwnershipTop100,
           Prev_OwnershipTop10 = sc_OwnershipTop10)

  starting_round_data <- master_table |>
    filter(Season == current_season, Round == 0) |>
    select(player_id,
           Starting_Price = sc_cost)

  price_and_ownership_chg <- current_round_data |>
    left_join(previous_round_data, by = "player_id") |>
    left_join(starting_round_data, by = "player_id") |>
    mutate(
      PriceChg = Price - Prev_Price,
      SeasonPriceChange = Price - Starting_Price,
      CBAchg = LastCBA - Prev_CBA,
      KIchg = LastKI - Prev_KI,
      OwnershipTop1000Chg = OwnershipTop1000 - Prev_OwnershipTop1000,
      OwnershipTop100Chg = OwnershipTop100 - Prev_OwnershipTop100,
      OwnershipTop10Chg = OwnershipTop10 - Prev_OwnershipTop10
    ) |>
    select(player_id, SeasonPriceChange, PriceChg, CBAchg, KIchg, OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg)

  # 2. Last PPM
  current_round_data <- current_round_data |>
    mutate(LastPPM = LastScore / minutes_played)

  # 3. Season Averages

  season_avg <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC)) |>
    group_by(player_id) |>
    summarise(
      SeasAvg = mean(SC, na.rm = TRUE),
      SeasTG = mean(TOG, na.rm = TRUE),
      SeasPPM = mean(SC / minutes_played, na.rm = TRUE),
      SeasCBA = mean(CBA_PERC, na.rm = TRUE),
      SeasKI = mean(KI, na.rm = TRUE),
      .groups = "drop"
    )



  season_avg_win <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC)) |>
    filter(result == "W") |>
    group_by(player_id) |>
    summarise(
      WinAvg = mean(SC, na.rm = TRUE),
      .groups = "drop"
    )

  season_avg_loss <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC)) |>
    filter(result == "L") |>
    group_by(player_id) |>
    summarise(
      LossAvg = mean(SC, na.rm = TRUE),
      .groups = "drop"
    )


  season_avg_home <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC)) |>
    filter(home_status == "home") |>
    group_by(player_id) |>
    summarise(
      HomeAvg = mean(SC, na.rm = TRUE),
      .groups = "drop"
    )


  season_avg_away <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC)) |>
    filter(home_status == "away") |>
    group_by(player_id) |>
    summarise(
      AwayAvg = mean(SC, na.rm = TRUE),
      .groups = "drop"
    )



  # 4. Last 3 Game Averages
  last3 <- master_table |>
    filter(Season == current_season) |>
    filter(!is.na(SC), !is.na(minutes_played)) |>
    arrange(player_id, desc(Season), desc(Round)) |>
    group_by(player_id) |>
    slice_head(n = 3) |>
    summarise(
      Last3Score = mean(SC, na.rm = TRUE),
      Last3TG = mean(TOG, na.rm = TRUE),
      Last3CBA = mean(CBA_PERC, na.rm = TRUE),
      Last3KI = mean(KI_PERC, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(Last3PPM = Last3Score/Last3TG)

  opp_table <- master_table |>
    filter(Opposition == next_opp) |>
    arrange(player_id, desc(Season), desc(Round)) |>
    group_by(player_id) |>
    mutate(row = row_number()) |>
    ungroup()


  opp_1 <- opp_table |>
    filter(row == 1) |>
    group_by(player_id) |>
    summarise(OppLastScore = mean(SC))

  opp_3 <- opp_table |>
    filter(row <= 3) |>
    group_by(player_id) |>
    summarise(OppLast3Score = mean(SC))

  opp_all <- opp_table |>
    group_by(player_id) |>
    summarise(OppCareer = mean(SC))



  # 5. Final Join
  final_table <- current_round_data |>
    left_join(price_and_ownership_chg, by = "player_id") |>
    left_join(season_avg, by = "player_id") |>
    left_join(last3, by = "player_id") |>
    left_join(season_avg_loss, by = "player_id") |>
    left_join(season_avg_win, by = "player_id") |>
    left_join(season_avg_home, by = "player_id") |>
    left_join(season_avg_away, by = "player_id") |>
    left_join(opp_1, by = "player_id") |>
    left_join(opp_3, by = "player_id") |>
    left_join(opp_all, by = "player_id") |>
    mutate(
      ReservesAvg = 0,
      ReservesLast = 0
    ) |>
    select(Player, Team, Position, SeasonPriceChange, Price, PriceChg, PricedAt, BreakEven,
           LastScore, LastTG, LastPPM, LastCBA, CBAchg, LastKI, KIchg,
           SeasAvg, SeasTG, SeasPPM, SeasCBA, SeasKI,
           Last3Score, Last3TG, Last3PPM, Last3CBA, Last3KI,
           OwnershipTotal, OwnershipTop1000, OwnershipTop100, OwnershipTop10,
           OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg,
           WinAvg, LossAvg, HomeAvg, AwayAvg,
           OppLastScore, OppLast3Score, OppCareer, ReservesAvg, ReservesLast)

  return(final_table)



}

af_pp <- af_price_projector(current_season = current_season, current_round = current_round)
sc_pp <- sc_price_projector(current_season = current_season, current_round = current_round)

af_team <- af_team_summary(current_season = current_season)
sc_team <- sc_team_summary(current_season = current_season)

af_big_table <- af_big_table(current_season = current_season, current_round = current_round)
sc_big_table <- sc_big_table(current_season = current_season, current_round = current_round)


cba_data <- master_table |>
  select(Season,
         roundNumber= Round,
         matchId,
         playerId= player_id,
         name= Player,
         TOG,
         AF,
         CBA,
         KI,
         teamName= team.name,
         CBA_PERC,
         KI_PERC,
         SC,
         TeamCBA,
         TeamKI) |>
  filter(!is.na(AF)) |>
  filter(Season >= 2021) |>
  mutate(teamName = if_else(teamName == "Adelaide Crows", "Adelaide",
                            if_else(teamName == "Footscray", "Western Bulldogs",
                                    if_else(teamName == "Gold Coast SUNS", "Gold Coast Suns",
                                            if_else(teamName == "GWS GIANTS", "GWS Giants", teamName))))) |>
  mutate(across(everything(), ~replace_na(., 0)))


cba_data <- cba_data |>
  filter(season == season & roundNumber == current_round)


data_prev_cba <- dbGetQuery(con, paste0("SELECT * FROM cba"))

cba_data <- cba_data |>
  select(all_of(names(data_prev_cba)))


cba_out <- rbind(cba_data, data_prev_cba)

# data_export

# # Find the maximum length
# max_len <- max(lengths(list(
#   names(master_table),
#   names(af_big_table),
#   names(sc_big_table),
#   names(af_pp),
#   names(sc_pp),
#   names(af_team),
#   names(sc_team),
#   names(cba_out)
# )))
#
# # Function to pad each vector
# pad_vec <- function(x, len) {
#   c(x, rep(NA, len - length(x)))
# }
#
# # Create the data.frame
# names_dt <- data.frame(
#   master = pad_vec(names(master_table), max_len),
#   player_af = pad_vec(names(af_big_table), max_len),
#   player_sc = pad_vec(names(sc_big_table), max_len),
#   proj_af = pad_vec(names(af_pp), max_len),
#   proj_sc = pad_vec(names(sc_pp), max_len),
#   team_af = pad_vec(names(af_team), max_len),
#   team_sc = pad_vec(names(sc_team), max_len),
#   cba = pad_vec(names(cba_out), max_len),
#   stringsAsFactors = FALSE
# )
#
#
# fwrite(names_dt, here("data","exports","2025","_for_mm","zz_adhoc",paste0("table_names.csv")))

# dbWriteTable(con, name = "master_table", value = master_table, row.names = FALSE, overwrite = TRUE)
# dbExecute(con, "DROP TABLE master_table")
# dbExecute(con, "DROP TABLE playerAF")
# dbExecute(con, "DROP TABLE playerSC")
# dbExecute(con, "DROP TABLE projectorSC")
# dbExecute(con, "DROP TABLE teamStatsAF")
# dbExecute(con, "DROP TABLE teamStatsSC")
# dbExecute(con, "DROP TABLE cba")


dbWriteTable(con, name = "master_table", value = master_table, row.names = FALSE,
             field.types = c(
               Season = "INT",
               Round = "INT",
               player_id = "VARCHAR(255)",
               Player = "VARCHAR(255)",
               squad_id = "INT",
               Team = "VARCHAR(10)",
               Position = "VARCHAR(10)",
               af_cost = "INT",
               sc_cost = "INT",
               minutes_played = "INT",
               SC = "INT",
               AF = "INT",
               matchId = "VARCHAR(255)",
               TOG = "INT",
               CBA = "INT",
               KI = "INT",
               CBA_PERC = "INT",
               KI_PERC = "INT",
               TeamCBA = "INT",
               TeamKI = "INT",
               team.name = "VARCHAR(255)",
               venue.name = "VARCHAR(255)",
               result = "VARCHAR(255)",
               sc_OwnershipTotal = "INT",
               sc_OwnershipTop1000 = "INT",
               sc_OwnershipTop100 = "INT",
               sc_OwnershipTop10 = "INT",
               af_OwnershipTotal = "INT",
               af_OwnershipTop1000 = "INT",
               af_OwnershipTop100 = "INT",
               af_OwnershipTop10 = "INT",
               af_magic_number = "INT",
               af_be = "INT",
               sc_magic_number = "INT",
               sc_be = "INT",
               next_opp = "VARCHAR(255)",
               af_priced_at = "INT",
               sc_priced_at = "INT",
               Opposition = "VARCHAR(255)",
               home_status = "VARCHAR(255)"
             ))

dbWriteTable(con, name = "playerStatsAF", value = af_big_table, row.names = FALSE,
             field.types = c(
               Player = "VARCHAR(255)",
               Team = "VARCHAR(10)",
               Position = "VARCHAR(10)",
               Price = "INT",
               SeasonPriceChange = "INT",
               PriceChg = "INT",
               PricedAt = "INT",
               BreakEven = "INT",
               LastScore = "INT",
               LastTG = "INT",
               LastPPM = "INT",
               LastCBA = "INT",
               CBAchg = "INT",
               LastKI = "INT",
               KIchg = "INT",
               SeasAvg = "INT",
               SeasTG = "INT",
               SeasPPM = "INT",
               SeasCBA = "INT",
               SeasKI = "INT",
               Last3Score = "INT",
               Last3TG = "INT",
               Last3PPM = "INT",
               Last3CBA = "INT",
               Last3KI = "INT",
               OwnershipTotal = "INT",
               OwnershipTop1000 = "INT",
               OwnershipTop100 = "INT",
               OwnershipTop10 = "INT",
               OwnershipTop1000Chg = "INT",
               OwnershipTop100Chg = "INT",
               OwnershipTop10Chg = "INT",
               WinAvg = "INT",
               LossAvg = "INT",
               HomeAvg = "INT",
               AwayAvg = "INT",
               OppLastScore = "INT",
               OppLast3Score = "INT",
               OppCareer = "INT",
               ReservesAvg = "INT",
               ReservesLast = "INT"
             ))

dbWriteTable(con, name = "playerStatsSC", value = sc_big_table, row.names = FALSE,
             field.types = c(
               Player = "VARCHAR(255)",
               Team = "VARCHAR(10)",
               Position = "VARCHAR(10)",
               Price = "INT",
               SeasonPriceChange = "INT",
               PriceChg = "INT",
               PricedAt = "INT",
               BreakEven = "INT",
               LastScore = "INT",
               LastTG = "INT",
               LastPPM = "INT",
               LastCBA = "INT",
               CBAchg = "INT",
               LastKI = "INT",
               KIchg = "INT",
               SeasAvg = "INT",
               SeasTG = "INT",
               SeasPPM = "INT",
               SeasCBA = "INT",
               SeasKI = "INT",
               Last3Score = "INT",
               Last3TG = "INT",
               Last3PPM = "INT",
               Last3CBA = "INT",
               Last3KI = "INT",
               OwnershipTotal = "INT",
               OwnershipTop1000 = "INT",
               OwnershipTop100 = "INT",
               OwnershipTop10 = "INT",
               OwnershipTop1000Chg = "INT",
               OwnershipTop100Chg = "INT",
               OwnershipTop10Chg = "INT",
               WinAvg = "INT",
               LossAvg = "INT",
               HomeAvg = "INT",
               AwayAvg = "INT",
               OppLastScore = "INT",
               OppLast3Score = "INT",
               OppCareer = "INT",
               ReservesAvg = "INT",
               ReservesLast = "INT"
             ))

dbWriteTable(con, name = "teamStatsAF", value = af_team, row.names = FALSE,
             field.types = c(
               Team = "VARCHAR(255)",
               Icon = "VARCHAR(10)",
               Last = "INT",
               LastOpp = "INT",
               Season = "INT",
               SeasonOpp = "INT",
               Last3 = "INT",
               Last3Opp = "INT",
               Top3Mid = "INT",
               Top3OppMid = "INT",
               TopRuc = "INT",
               TopOppRuc = "INT",
               Top2Def = "INT",
               Top2OppDef = "INT",
               Top2For = "INT",
               Top2OppFor = "INT"
             ))

dbWriteTable(con, name = "teamStatsSC", value = sc_team, row.names = FALSE,
             field.types = c(
               Team = "VARCHAR(255)",
               Icon = "VARCHAR(10)",
               Last = "INT",
               LastOpp = "INT",
               Season = "INT",
               SeasonOpp = "INT",
               Last3 = "INT",
               Last3Opp = "INT",
               Top3Mid = "INT",
               Top3OppMid = "INT",
               TopRuc = "INT",
               TopOppRuc = "INT",
               Top2Def = "INT",
               Top2OppDef = "INT",
               Top2For = "INT",
               Top2OppFor = "INT"
             ))

dbWriteTable(con, name = "cba", value = cba_out, row.names = FALSE,
             field.types = c(
               Season = "INT",
               roundNumber = "INT",
               matchId = "VARCHAR(255)",
               playerId = "VARCHAR(255)",
               name = "VARCHAR(255)",
               TOG = "INT",
               AF = "INT",
               CBA = "INT",
               KI = "INT",
               teamName = "VARCHAR(255)",
               CBA_PERC = "INT",
               KI_PERC = "INT",
               SC = "INT",
               TeamCBA = "INT",
               TeamKI = "INT"
             ))

dbGetQuery(con, "SELECT * FROM cba") |> nrow()

# dbWriteTable(con, name = "projectorAF", value = af_pp, row.names = FALSE, overwrite = TRUE,
#              field.types = c(
#
#              ))
# dbWriteTable(con, name = "projectorSC", value = sc_pp, row.names = FALSE, overwrite = TRUE,
#              field.types = c(
#
#              ))












































