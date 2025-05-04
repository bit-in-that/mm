box::use(
  httr2[...]
)

request_authorised <- function(api_url, access_token) {
  headers <- list(authorization = paste0("Bearer ", access_token))

  request(api_url) |>
    req_headers(!!!headers)

}

# TODO: make default year no longer hard-coded to be 2025
# https://www.supercoach.com.au/2025/api/afl/classic/v1/players-cf?embed=notes,odds,player_stats,positions
#' @export
get_players <- function(round = NULL, year = 2025, embed = "notes,odds,player_stats,player_match_stats,positions") {
  if(is.null(year)) {
    year <- Sys.time()
  }
  params <- list(embed = embed, round = round)
  paste0("https://www.supercoach.com.au/", year, "/api/afl/classic/v1/players-cf") |>
    request() |>
    req_url_query(!!!params) |>
    req_perform() |>
    resp_body_json()

}

# TODO: add triangulation and pipeline for this
#' @export
get_individual_players <- function(player_id, year = 2025, embed = "notes,odds,player_stats,player_match_stats,positions,trades") {
  if(is.null(year)) {
    year <- Sys.time()
  }
  params <- list(embed = embed)
  paste0("https://www.supercoach.com.au/", year, "/api/afl/classic/v1/players/", player_id) |>
    request() |>
    req_url_query(!!!params) |>
    req_perform() |>
    resp_body_json()

}

# TODO: make default year no longer hard-coded to be 2025
#' @export
request_rankings <- function(access_token, page = 1, page_size = 100, period = "overall", round = NULL, year = 2025) {
  params <- list(period = period, round = round, page =page, page_size = page_size)
  request_authorised(paste0("https://www.supercoach.com.au/", year, "/api/afl/classic/v1/rankings/userteams/all"), access_token) |>
    req_url_query(!!!params)

}

#' @export
get_rankings <- function(access_token, page = 1, page_size = 100, period = "overall", round = NULL, year = 2025) {
  request_rankings(access_token, page, page_size, period, round, year) |>
    req_perform() |>
    resp_body_json()

}

#' @export
request_team <- function(access_token, team_id, round = NULL, year = 2025) {
  params <- list(round = round)
  request_authorised(paste0("https://www.supercoach.com.au/", year, "/api/afl/classic/v1/userteams/", team_id, "/statsPlayers"), access_token) |>
    req_url_query(!!!params)

}

#' @export
get_team <- function(access_token, team_id, round = NULL, year = 2025) {
  request_team(access_token, team_id, round, year)  |>
    req_perform() |>
    resp_body_json()

}
