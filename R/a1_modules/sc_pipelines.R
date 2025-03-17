box::use(
  dplyr[...],
  purrr[...],
  httr2[...]
)

box::use(
  ./sc_api,
  ./sc_tabulate
)

#' @export
players <- function(round = NULL, year = 2025, embed = "positions") {
  sc_api$get_players(round, year, embed) |>
    sc_tabulate$players() |>
    rename(player_id = id)

}

#' @export
players_stats <- function(round = NULL, year = 2025, embed = "player_stats") {
  sc_api$get_players(round, year, embed) |>
    sc_tabulate$player_stats()

}

#' @export
players_notes <- function(round = NULL, year = 2025, embed = "notes") {
  sc_api$get_players(round, year, embed) |>
    sc_tabulate$players_notes()

}

#' @export
rankings <- function(access_token, pages = 1, page_size = 100, period = "overall", round = NULL, year = 2025) {
  base_query <- sc_api$request_rankings(access_token, page = 1, page_size, period, round, year)

  req_list <- pages |>
    map(~{
      base_query |>
        req_url_query(page = .x)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        sc_tabulate$rankings()
    })

}


#' @export
team_lineups <- function(access_token, team_ids, round = NULL, year = 2025) {
  req_list <- team_ids |>
    map(~{
      sc_api$request_team(access_token, team_id = .x, round, year)
    })

  resp_list <- req_list |>
    req_perform_parallel(on_error = "continue")

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        sc_tabulate$team_lineup()
    })

}
