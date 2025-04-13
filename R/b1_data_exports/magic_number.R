
box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines
)

box::use(
  dplyr[...],
  purrr[...]
)

#' @export
af_magic_number <- function(c_round) {
  af_players_by_round <- af_pipelines$players_by_round()
  magic_number_r0 <- 10267.1755725191

  af_players_by_round_avg <- af_players_by_round |>
    arrange(player_id, round) |>
    group_by(player_id) |>
    mutate(
      score_rolling_avg = map_dbl(row_number(), function(i) {
        valid_scores <- score[1:i][!is.na(score[1:i])]
        if (length(valid_scores) == 0) NA_real_ else mean(valid_scores)
      })
    ) |>
    ungroup()

  data <- af_players_by_round |>
    arrange(player_id, round) |>
    left_join(
      af_players_by_round_avg |>
        select(player_id, round, score_rolling_avg),
      by = c("player_id", "round")
    ) |>
    mutate(
      lag_price = lead(price),
      season_avg = if_else(
        is.na(score_rolling_avg),
        lag_price / magic_number_r0,
        score_rolling_avg
      )
    )

  sum_price <- data |>
    filter(round == c_round) |>
    pull(lag_price) |>
    sum(na.rm = TRUE)

  sum_avg <- data |>
    filter(round == c_round) |>
    pull(season_avg) |>
    sum(na.rm = TRUE)

  return(sum_price / sum_avg)
}




#' @export
sc_magic_number <- function(c_round) {

  # c_round <- 5

  sc_magic_number_r0 <- 5387.372

  data <- sc_pipelines$players_stats(round = c_round)

  data <- data |>
    mutate(season_avg = total_points/total_games) |>
    mutate(
      season_avg = if_else(
        total_games == 0,
        price / sc_magic_number_r0,
        season_avg
      ))

  sum_price <- data |>
    filter(round == c_round) |>
    pull(price) |>
    sum(na.rm = TRUE)

  sum_avg <- data |>
    filter(round == c_round) |>
    pull(season_avg) |>
    sum(na.rm = TRUE)

  return(sum_price / sum_avg)


}


