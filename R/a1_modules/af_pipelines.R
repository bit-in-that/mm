box::use(
  dplyr[...],
  purrr[...],
  httr2[...],
  utils[tail],
  readxl[read_excel],
  here[here]
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
squads <- function(apply_adhoc_changes = FALSE) {


  squads <- af_api$get_squads() |>
    af_tabulate$squads() |>
    rename(squad_id = id)

  if(apply_adhoc_changes) {
    adhoc_changes_team_names_short <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "team_names_short")
    adhoc_changes_team_names <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "team_names")

    # the logic below depends on this being, true, if it is not the case please amend the code:
    stopifnot(squads$full_name == squads$name)

    squads <- squads |>
      left_join(
        adhoc_changes_team_names_short, by = c("short_name" = "afl")
      ) |>
      mutate(
        short_name = coalesce(mm, short_name)
      ) |>
      select(-mm) |>
      left_join(
        adhoc_changes_team_names, by = c("full_name" = "afl")
      ) |>
      mutate(
        full_name = coalesce(mm, full_name),
        name = coalesce(mm, name)
      ) |>
      select(-mm)
  }

  return(squads)
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
team_ranks_by_round <- function(session_id, user_ids) {
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
        af_tabulate$team_ranks_by_round()
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

  resp_list |>
    resps_successes() |>
    resps_data(\(resp) {
      resp |>
        resp_body_json() |>
        af_tabulate$team_lineup(team_round = round_num)
    }) |>
    group_by(team_id, round, line_name) |>
    mutate(
      is_utility = (utility_position == line_name) & (player_id == tail(player_id, 1))
    ) |>
    ungroup() |>
    select(
      -utility_position
    )
}


#' @export
teams <- function(session_id, team_ids, round_num = NULL) {
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
        af_tabulate$team()
    })
}

#' @export
team_value <- function(session_id, team_ids, round_num = NULL) {
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
        af_tabulate$team()
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


team_value_previous_round <- function(session_id, team_ids, round_num = NULL) {
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
        af_tabulate$team_value_previous_round(prices_round = round_num)
    })

}

team_value_current_round <- function(session_id, team_ids, round_num = NULL, players_by_round = NULL) {
  if(is.null(round_num)) {
    round_num <- current_round() + 1
  }
  if(is.null(players_by_round)) {
    players_by_round <- players_by_round()
  }

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
        af_tabulate$team_value_current_round(players_by_round = players_by_round, prices_round = round_num)
    })

}

#' @export
team_value <- function(session_id, team_ids, rounds = NULL, include_current_round = TRUE, players_by_round = NULL) {

  if(is.null(rounds)) {
    rounds <- seq(current_round())
  }
  output <- rounds |>
    map(team_value_previous_round, session_id = session_id, team_ids = team_ids) |>
    list_rbind()

  if(include_current_round) {
    if(is.null(players_by_round)) {
      players_by_round <- players_by_round()
    }

    output <- bind_rows(
      output,
      team_value_current_round(session_id, team_ids, players_by_round = players_by_round, round_num = max(rounds) + 1)
    )
  }

  output

}

