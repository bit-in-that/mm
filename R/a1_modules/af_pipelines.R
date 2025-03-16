box::use(
  dplyr[...],
  purrr[...],
  httr2[...]
)

box::use(
  ./af_api,
  ./af_tabulate
)

#' @export
players <- function() {

  players_df <- af_api$get_players() |>
    af_tabulate$players()

  players_coach_df <- af_api$get_players_coach() |>
    af_tabulate$players_coach()

  stopifnot(nrow(players_coach_df) == nrow(players_df))

  left_join(players_df, players_coach_df, by = "id") |>
    rename(player_id = id)

}

#' @export
players_by_round <- function() {

  players_df <- af_api$get_players() |>
    af_tabulate$players_by_round()

  players_coach_df <- af_api$get_players_coach() |>
    af_tabulate$players_coach_by_round()

  full_join(players_df, players_coach_df, by = c("player_id", "round"))
}

#' @export
squads <- function() {
  af_api$get_squads() |>
    af_tabulate$squads() |>
    rename(squad_id = id)
}

#' @export
team_count <- function(session_id) {
  af_api$get_team_rank(session_id)$result$num_teams
}

#' @export
team_user_ids <- function(session_id, team_ids, round_num = NULL) {
  base_query <- af_api$request_team(session_id, team_id = NULL, round_num = round_num)

  req_list <- team_ids |>
    map(~{
      base_query |>
        req_url_query(id = .x)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        af_tabulate$team_user_id()
    })
}

#' @export
team_ranks <- function(session_id, user_ids) {
  base_query <- af_api$request_team_rank(session_id, user_id = NULL)

  req_list <- user_ids |>
    map(~{
      base_query |>
        req_url_query(user_id = .x)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        af_tabulate$team_rank()
    })
}

#' @export
team_lineups <- function(session_id, team_ids, round_num = NULL) {
  base_query <- af_api$request_team(session_id, team_id = NULL, round_num = round_num)

  req_list <- team_ids |>
    map(~{
      base_query |>
        req_url_query(id = .x)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  if(is.null(round_num)) {
    extra_args <- list()
  } else {
    extra_args <- list(team_round = round_num)
  }

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        af_tabulate$team_lineup(team_round = NULL)
    })
}

#' @export
rounds <- function() {
  af_api$get_rounds() |>
    af_tabulate$rounds()

}

#' @export
current_round <- function() {
  rounds() |>
    filter(status != "scheduled") |>
    pull(round) |>
    max()
}

#' @export
max_rankings_offsets <- (0:50)*50

#' @export
rankings <- function(session_id, offsets = max_rankings_offsets, order = c("rank", "avg_points", "round_points", "highest_round_score", "team_value"), order_direction = c("ASC", "DESC"), round = NULL, club = NULL, state = NULL) {

  base_query <- af_api$request_rankings(session_id, offset = 0, order = order, order_direction = order_direction, round = round, club = club, state = state)

  req_list <- offsets |>
    map(~{
      base_query |>
        req_url_query(offset = .x)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        af_tabulate$rankings()
    })
}
