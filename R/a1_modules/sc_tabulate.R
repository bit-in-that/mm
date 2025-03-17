box::use(
  dplyr[...],
  purrr[...]
)

#' @export
players <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player) |>
    bind_rows()
}

#' @export
players_notes <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player_notes) |>
    bind_rows()
}

#' @export
player_stats <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player_stats) |>
    bind_rows()
}

clean_player <- function(player) {
  team <- player$team

  c(
    player[c("id", "first_name", "last_name", "feed_id", "locked", "active")],
    player[c("hs_url", "injury_suspension_status", "injury_suspension_status_text")] |> map_if(is.null, ~NA_character_),
    player[c("previous_games", "previous_total")] |> map_if(is.null, ~NA_integer_),
    player[c("previous_average")] |> map_if(is.null, ~NA_real_),
    team |> `names<-`(paste0("team_", names(team))),
    list(
      position = player$positions[[1]]$position,
      position_long = player$positions[[1]]$position_long,
      position_sort = player$positions[[1]]$sort,
      player_status = player$played_status$status,
      player_status_display = player$played_status$display
    )
  ) |>
    as_tibble()

}

clean_player_notes <- function(player) {
  player$notes |>
    map(as_tibble) |>
    bind_rows()
}

clean_player_stats <- function(player) {
  player$player_stats |>
    map(clean_player_stats_single) |>
    list_rbind()
}

clean_player_stats_single <- function(player_stats) {

  player_stats[c("player_id", "round", "points", "price", "total_points", "position", "games", "total_games", "price_change", "total_price_change", "last_position", "round_position", "minutes_played", "total_minutes_played", "updated_at", "kicks", "total_kicks", "handballs", "total_handballs", "marks", "total_marks", "tackles", "total_tackles", "freekicks_for", "total_freekicks_for", "freekicks_against", "total_freekicks_against", "hitouts", "total_hitouts", "goals", "total_goals" , "behinds", "total_behinds", "avg5", "avg", "avg3", "own_raw", "livepts", "livegames", "owned")] |>
  as_tibble()

}

# TODO: add functions for getting more general team info than just the lineup
#' @export
team_lineup <- function(resp_body_team) {
  resp_body_team$players |>
    map(as_tibble) |>
    list_rbind()
}


#' @export
rankings <- function(resp_body_rankings) {
  resp_body_rankings |>
    map(clean_ranking) |>
    list_rbind()
}

# TODO: get more information from here
clean_ranking <- function(ranking) {
  tibble(
    raw_position = ranking$raw_position,
    position = ranking$position,
    team_id = ranking$userTeam$id,
    user_id = ranking$userTeam$user_id
    )

}
