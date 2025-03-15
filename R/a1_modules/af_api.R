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

