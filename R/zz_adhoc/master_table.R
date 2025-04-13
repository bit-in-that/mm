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
  data.table[fwrite]
)

# define season
season <- 2025
current_round <- af_pipelines$current_round()
# current_round <- 5

# read in previous
data_prev <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round-1),paste0("master_table_r_",current_round-1,".csv")))
data_curr <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("mm_master_table_r_",current_round,".csv")))

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

  price_and_ownership_chg <- current_round_data |>
    left_join(previous_round_data, by = "player_id") |>
    mutate(
      PriceChg = Price - Prev_Price,
      CBAchg = LastCBA - Prev_CBA,
      KIchg = LastKI - Prev_KI,
      OwnershipTop1000Chg = OwnershipTop1000 - Prev_OwnershipTop1000,
      OwnershipTop100Chg = OwnershipTop100 - Prev_OwnershipTop100,
      OwnershipTop10Chg = OwnershipTop10 - Prev_OwnershipTop10
    ) |>
    select(player_id, PriceChg, CBAchg, KIchg, OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg)

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

  player_stats_2025 <- fetch_player_stats_afl(season = current_season)
  next_round <- current_round+1
  upcoming_fix <- player_stats_2025 |>
    filter(round.roundNumber == next_round) |>
    distinct(home.team.name, away.team.name)
  upcoming_fix_flipped <- upcoming_fix |>
    mutate(team = away.team.name,
           team_2 = home.team.name) |>
    select(home.team.name = team,
           away.team.name = team_2)

  next_fix <- rbind(upcoming_fix, upcoming_fix_flipped) |>
    rename(team.name = home.team.name,
           next_opp = away.team.name)

  map <- master_table |>
    filter(Season == current_season &Round == current_round) |>
    left_join(next_fix,
              by = "team.name") |>
    select(player_id, Player, next_opp, Team) |>
    distinct(Team, next_opp) |>
    filter(!is.na(next_opp))

  player_map <- master_table |>
    filter(Season == current_season) |>
    left_join(map,
              by = "Team") |>
    distinct(player_id, next_opp)


  opp_table <- master_table |>
    left_join(player_map,
              by = "player_id") |>
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
    select(Player, Team, Position, Price, PriceChg, PricedAt, BreakEven,
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

  price_and_ownership_chg <- current_round_data |>
    left_join(previous_round_data, by = "player_id") |>
    mutate(
      PriceChg = Price - Prev_Price,
      CBAchg = LastCBA - Prev_CBA,
      KIchg = LastKI - Prev_KI,
      OwnershipTop1000Chg = OwnershipTop1000 - Prev_OwnershipTop1000,
      OwnershipTop100Chg = OwnershipTop100 - Prev_OwnershipTop100,
      OwnershipTop10Chg = OwnershipTop10 - Prev_OwnershipTop10
    ) |>
    select(player_id, PriceChg, CBAchg, KIchg, OwnershipTop1000Chg, OwnershipTop100Chg, OwnershipTop10Chg)

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

  player_stats_2025 <- fetch_player_stats_afl(season = current_season)
  next_round <- current_round+1
  upcoming_fix <- player_stats_2025 |>
    filter(round.roundNumber == next_round) |>
    distinct(home.team.name, away.team.name)
  upcoming_fix_flipped <- upcoming_fix |>
    mutate(team = away.team.name,
           team_2 = home.team.name) |>
    select(home.team.name = team,
           away.team.name = team_2)

  next_fix <- rbind(upcoming_fix, upcoming_fix_flipped) |>
    rename(team.name = home.team.name,
           next_opp = away.team.name)

  map <- master_table |>
    filter(Season == current_season &Round == current_round) |>
    left_join(next_fix,
              by = "team.name") |>
    select(player_id, Player, next_opp, Team) |>
    distinct(Team, next_opp) |>
    filter(!is.na(next_opp))

  player_map <- master_table |>
    filter(Season == current_season) |>
    left_join(map,
              by = "Team") |>
    distinct(player_id, next_opp)


  opp_table <- master_table |>
    left_join(player_map,
              by = "player_id") |>
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
    select(Player, Team, Position, Price, PriceChg, PricedAt, BreakEven,
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


# data_export


fwrite(af_pp, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("projectorAF.csv")))
fwrite(sc_pp, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("projectorSC.csv")))
fwrite(af_team, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("teamStatsAF.csv")))
fwrite(sc_team, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("teamStatsSC.csv")))
fwrite(af_big_table, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("playerAF.csv")))
fwrite(sc_big_table, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("playerSC.csv")))
fwrite(cba_data, here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("cba.csv")))



















































