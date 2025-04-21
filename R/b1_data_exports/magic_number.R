
box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines
)

box::use(
  dplyr[...],
  purrr[...],
  readr[read_csv],
  here[here]
)

#' @export
af_magic_number <- function(c_round) {

  af_players_by_round <- af_pipelines$players_by_round()

  af_players_by_round <- af_players_by_round |>
    mutate(player_id = if_else(player_id == 1026850, 1012013, player_id))


  map <- data.frame(games_played = c(1:5),
                    wt = 0.25*c(5/15,
                                4/15,
                                3/15,
                                2/15,
                                1/15))
  weights <- 0.25 * c(4 / 15, 3 / 15, 2 / 15, 1 / 15)

  # c_round <- 4


  mn_data <- af_players_by_round |>
    arrange(player_id, round) |>
    mutate(lag_price = lead(price)) |>
    select(player_id, round, price, score, lag_price)

  # shouldn't need this unless its mid round
  mn_data <- mn_data |>
    filter(round <= c_round)

  processed_data <- mn_data |>
    filter(!is.na(score)) |>
    group_by(player_id) |>
    mutate(season_avg = sum(score)/n()) |>
    ungroup() |>
    filter(round == c_round)

  sum_price <- processed_data |>
    pull(lag_price) |>
    sum()

  sum_avg <- processed_data |>
    pull(season_avg) |>
    sum()

  af_mn <- (sum_price / sum_avg)

  data <- af_players_by_round |>
    arrange(player_id, round) |>
    mutate(lag_price = lead(price)) |>
    mutate(be = lead(break_even)) |>
    select(player_id, round, price, score, lag_price, be)


  data <- data |>
    filter(round <= c_round)

  last_scores <- data |>
    filter(!is.na(score)) |>
    group_by(player_id) |>
    arrange(player_id, desc(round)) |>
    mutate(row_n = row_number()) |>
    ungroup() |>
    mutate(last_game = paste0("L0", row_n)) |>
    filter(row_n <= 4) |>
    select(player_id, score, last_game)

  last_scores_wide <- last_scores |>
    tidyr::pivot_wider(
      names_from = last_game,
      values_from = score,
      names_sort = TRUE
    )

  prices <- data |>
    filter(round == c_round) |>
    select(player_id, af_price = lag_price)


  dt <- data |>
    select(player_id) |>
    mutate(mn = af_mn)

  map <- map |>
    mutate(wt_cum = cumsum(wt))

  be <- data |>
    filter(!is.na(be)) |>
    select(player_id, be)

  dt_stats <- dt |>
    left_join(last_scores_wide, by = "player_id") |>
    left_join(prices, by = "player_id") |>
    mutate(games_played = 5 - rowSums(is.na(across(L01:L04)))) |>
    left_join(map, by = "games_played")


  dt_be <- dt_stats |>
    mutate(across(L01:L04, ~ tidyr::replace_na(.x, 0), .names = "clean_{.col}"),
           weighted_sum = as.numeric(as.matrix(pick(clean_L01, clean_L02, clean_L03, clean_L04)) %*% weights),
           af_be = -((af_price * (1 - wt_cum) + weighted_sum * mn - af_price) / (mn / 12))) |>
    left_join(be, by = "player_id") |>
    mutate(diff = af_be - be)

  af_out <- dt_be |>
    mutate(af_be = round(af_be,0)) |>
    mutate(player_id = paste0("CD_I", player_id)) |>
    distinct(player_id, mn, af_be)

  return(af_out)


}

#' @export
sc_magic_number <- function(c_round, c_season) {

  # c_round <- 5
  # c_season <- 2025

  sc_data <- sc_pipelines$players_stats(round = c_round)
  sc_players <- sc_pipelines$players()


  mn_data <- sc_data |>
    mutate(season_avg = total_points/total_games) |>
    filter(games == 1)

  sum_price <- mn_data |>
    pull(price) |>
    sum(na.rm = TRUE)

  sum_avg <- mn_data |>
    pull(season_avg) |>
    sum(na.rm = TRUE)

  sc_mn <- (sum_price / sum_avg)


  map <- sc_players |>
    select(player_id, feed_id)

  data <- sc_data |>
    left_join(map, by = "player_id") |>
    mutate(player_id = paste0("CD_I", feed_id))

  data_curr <- data |>
    filter(games == 1) |>
    select(player_id, Round = round, SC = points)

  # turn this into an SQL query
  data_prev <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0", c_round-1),paste0("master_table_r_",c_round-1,".csv")))

  data_prev <- data_prev |>
    filter(Season == c_season) |>
    select(player_id, Round, SC)

  points_data <- rbind(data_curr, data_prev)

  last_scores <- points_data |>
    filter(!is.na(SC)) |>
    group_by(player_id) |>
    arrange(player_id, desc(Round)) |>
    mutate(row_n = row_number()) |>
    ungroup() |>
    mutate(last_game = paste0("L0", row_n)) |>
    filter(row_n <= 2) |>
    select(player_id, SC, last_game)

  last_scores_wide <- last_scores |>
    tidyr::pivot_wider(
      names_from = last_game,
      values_from = SC,
      names_sort = TRUE
    )

  prices <- data |>
    select(player_id, sc_price = price)

  dt <- data |>
    select(player_id) |>
    mutate(mn = sc_mn)

  dt_stats <- dt |>
    left_join(last_scores_wide, by = "player_id") |>
    left_join(prices, by = "player_id") |>
    mutate(games_played = 3 - rowSums(is.na(across(L01:L02))))


  dt_be <- dt_stats |>
    mutate(across(L01:L02, ~ tidyr::replace_na(.x, 0), .names = "clean_{.col}")) |>
    mutate(sc_be = if_else(is.na(L01),
                           (3*sc_price/mn - clean_L01)/(4-games_played)*(1+0.02),
                           if_else(is.na(L02), 2*sc_price/mn - clean_L01,
                                   3*sc_price/mn - clean_L01 - clean_L02)))

  sc_out <- dt_be |>
    mutate(sc_be = round(sc_be,0)) |>
    select(player_id, sc_mn = mn, sc_be)

  return(sc_out)


}


