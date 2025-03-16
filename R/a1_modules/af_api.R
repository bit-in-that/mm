# Verb prefix meanings:
# - get_: providing the json response as a list from an api
# - request_: the httr2 request object for the api (still possible to modify further and hit yourself in a loop)
#
# Note that only get_ is provided for apis where request_ will never need to be used (e.g. players)
box::use(
  httr2[request, req_perform, resp_body_json, req_headers, req_url_query]
)

# req_url_query

request_authorised <- function(api_url, session_id) {
  headers <- list(cookie = paste0("session=", session_id))

  request(api_url) |>
    req_headers(!!!headers)

}

#' @export
get_players_coach <- function() {
  request("https://fantasy.afl.com.au/data/afl/coach/players.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_players <- function() {
  request("https://fantasy.afl.com.au/data/afl/players.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_rounds <- function() {
  request("https://fantasy.afl.com.au/data/afl/rounds.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_squads <- function() {
  request("https://fantasy.afl.com.au/data/afl/squads.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_venues <- function() {
  request("https://fantasy.afl.com.au/data/afl/venues.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_squads <- function() {
  request("https://fantasy.afl.com.au/data/afl/squads.json") |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_player_single <- function(player_id) {
  request(paste0("https://fantasy.afl.com.au/data/afl/stats/players/", player_id, ".json")) |>
    req_perform() |>
    resp_body_json()

}

#' @export
request_team <- function(session_id, team_id, round_num = NULL) {
  params <- list(round_num = NULL, id = team_id)

  request_authorised("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show", session_id) |>
    req_url_query(!!!params)

}

#' @export
get_team <- function(session_id, team_id, round_num = NULL) {
  request_team(session_id, team_id, round_num) |>
    req_perform() |>
    resp_body_json()

}

#' @export
get_user <- function(session_id) {
  request_authorised("https://fantasy.afl.com.au/afl_classic/api/user", session_id) |>
    req_perform() |>
    resp_body_json()

}

#' @export
request_team_rank <- function(session_id, user_id = NULL) {
  params <- list(user_id = user_id)

  request_authorised("https://fantasy.afl.com.au/afl_classic/api/teams_classic/snapshot", session_id) |>
    req_url_query(!!!params)

}

#' @export
get_team_rank <- function(session_id, user_id = NULL) {
  request_team_rank(session_id, user_id) |>
    req_perform() |>
    resp_body_json()

}

#' @export
request_rankings <- function(session_id, offset = 0, order = c("rank", "avg_points", "round_points", "highest_round_score", "team_value"), order_direction = c("DESC", "ASC"), round = NULL, club = NULL, state = NULL) {
  order <- order[1]
  order_direction <- order_direction[1]

  params <- list(offset = offset, order = order, order_direction = order_direction, round = round, club = club, state = state)

  request_authorised("https://fantasy.afl.com.au/afl_classic/api/teams_classic/rankings", session_id) |>
    req_url_query(!!!params)

}

#' @export
get_rankings <- function(session_id, offset = 0, order = c("rank", "avg_points", "round_points", "highest_round_score", "team_value"), order_direction = c("ASC", "DESC"), round = NULL, club = NULL, state = NULL) {
  # club is squad_id
  # state is: c("0" = "All States", "1" = "Australian Capital Territory", "2" = "Northern Territory", "3" = "New South Wales", "4" = "Queensland", "5" = "South Australia", "6" = "Tasmania", "7" = "Victoria", "8" = "Western Australia")
  order <- order[1]
  order_direction <- order_direction[1]

  request_rankings(session_id, offset, order, order_direction, round, club, state) |>
    req_perform() |>
    resp_body_json()

}


