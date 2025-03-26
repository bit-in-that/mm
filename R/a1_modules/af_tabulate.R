# The clean_ functions below are not exported because they are just helper functions
# components skipped: last_season_scores from coach version
# TODO: add is emergency and is utility to lineup
box::use(
  dplyr[...],
  purrr[...]
)

#' @export
players <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player) |>
    list_rbind()
}

#' @export
players_coach <- function(resp_body_players_coach) {
  player_ids <- resp_body_players_coach |>
    names() |>
    as.integer()

  resp_body_players_coach |>
    map2(player_ids, clean_player_coach) |>
    list_rbind()
}

#' @export
players_career_avg_vs <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player_career_avg_vs) |>
    list_rbind()

}

#' @export
players_by_round <- function(resp_body_players) {
  resp_body_players |>
    map(clean_player_by_round) |>
    list_rbind()

}

#' @export
players_coach_venues <- function(resp_body_players_coach) {
  player_ids <- resp_body_players_coach |>
    names() |>
    as.integer()

  resp_body_players_coach |>
    map2(player_ids, clean_player_coach_venues) |>
    list_rbind()

}

#' @export
players_coach_opponents <- function(resp_body_players_coach) {
  player_ids <- resp_body_players_coach |>
    names() |>
    as.integer()
  resp_body_players_coach |>
    map2(player_ids, clean_player_coach_opponents) |>
    list_rbind()

}

#' @export
players_coach_by_round <- function(resp_body_players_coach) {
  player_ids <- resp_body_players_coach |>
    names() |>
    as.integer()
  resp_body_players_coach |>
    map2(player_ids, clean_player_coach_by_round) |>
    list_rbind()

}


clean_player <- function(player) {
  player_names <- c("id", "first_name", "last_name", "slug", "dob", "squad_id", "cost", "status", "is_bye", "locked")
  stats_names <- c("season_rank", "games_played", "total_points", "high_score", "low_score", "last_3_avg", "last_5_avg", "selections", "owned_by", "adp", "proj_avg", "rd_tog", "tog", "career_avg", "leagues_rostered", "last_3_proj_avg")
  selections_info_names <- c("c","vc","bc","emg")

  c(
    player[player_names],
    clean_player_position(player$positions),
    clean_player_position(player$original_positions, suffix = "_original"),
    player$stats[stats_names],
    player$stats$selections_info[selections_info_names] |>
      `names<-`(paste0("selections_info_", selections_info_names))
  ) |>
    as_tibble() |>
    mutate(
      rd_tog = as.integer(rd_tog)
    )
}

clean_player_coach <- function(player, player_id) {
  c(
    list(id = player_id),
    player[c("last_3_proj_avg", "last_3_tog_avg", "consistency", "in_20_avg", "out_20_avg", "draft_selections", "proj_score", "break_even", "last_5_tog_avg")]
  ) |>
    as_tibble()
}

clean_player_career_avg_vs <- function(player) {
  career_avg_vs <- player$stats$career_avg_vs
  if(length(career_avg_vs) == 0) {
    NULL
  } else {
    tibble(
      player_id = player$id,
      squad_id = names(career_avg_vs),
      career_avg = unname(unlist(career_avg_vs))
    )

  }
}

clean_player_by_round <- function(player) {

  prices <- unlist(player$stats$prices)
  scores <- unlist(player$stats$scores)
  ranks <- unlist(player$stats$ranks)

  rounds <- c(names(prices), names(scores), names(ranks)) |>
    unique()

  if(is.null(prices)) {
    prices <- NA_integer_
  }

  if(is.null(scores)) {
    scores <- NA_integer_
  }

  if(is.null(ranks)) {
    ranks <- NA_integer_
  }

  tibble(
    player_id = player$id,
    round = as.integer(rounds),
    price = prices[rounds],
    score = scores[rounds],
    rank = ranks[rounds]
  ) |>
    arrange(round)


}

clean_player_coach_venues <- function(player, player_id) {
  avg_at_venues <- player$venues
  if(length(avg_at_venues) == 0) {
    NULL
  } else {
    tibble(
      player_id = player_id,
      venue_id = as.integer(names(avg_at_venues)),
      career_avg = unname(unlist(avg_at_venues))
    )

  }
}

clean_player_coach_opponents <- function(player, player_id) {
  avg_opponents <- player$opponents
  if(length(avg_opponents) == 0) {
    NULL
  } else {
    tibble(
      player_id = player_id,
      venue_id = as.integer(names(avg_opponents)),
      avg_opponents = unname(unlist(avg_opponents))
    )

  }
}

clean_player_coach_by_round <- function(player, player_id) {
  proj_scores <- unlist(player$proj_scores)
  proj_prices <- unlist(player$proj_prices)
  break_evens <- unlist(player$break_evens)
  be_pct <- unlist(player$be_pct)

  transfers <- player$transfers

  rounds <- c(names(proj_scores), names(proj_prices), names(break_evens), names(be_pct), names(transfers)) |>
    unique()

  if(is.null(proj_scores)) {
    proj_scores <- NA_integer_
  }

  if(is.null(proj_prices)) {
    proj_prices <- NA_integer_
  }

  if(is.null(break_evens)) {
    break_evens <- NA_integer_
  }

  if(is.null(be_pct)) {
    be_pct <- NA_integer_
  }

  if(length(transfers) != 0) {
    transfers_in <- transfers |>
      map_int(pluck, "in")
    transfers_out <- transfers |>
      map_int(pluck, "out")

  } else {
    transfers_in <- NA_integer_
    transfers_out <- NA_integer_

  }

  tibble(
    player_id = player_id,
    round = as.integer(rounds),
    proj_score = proj_scores[rounds],
    proj_price = proj_prices[rounds],
    break_even = break_evens[rounds],
    be_pct = be_pct[rounds],
    transfers_in = transfers_in[rounds],
    transfers_out = transfers_out[rounds]
  ) |>
    arrange(round)
}

clean_player_position <- function(positions, suffix = "") {
  positions <- unlist(positions)
  position <- paste0(c("Def", "Mid", "Ruc", "Fwd")[positions], collapse = "/")
  output <- list(
    position = position,
    is_defender = 1 %in% positions,
    is_midfielder = 2 %in% positions,
    is_ruck = 3 %in% positions,
    is_forward = 4 %in% positions
  )
  names(output) <- paste0(names(output), suffix)
  output

}


#' @export
squads <- function(resp_body_squads) {
  resp_body_squads |>
    map(as_tibble) |>
    bind_rows()

}

#' @export
team <- function(resp_body_team) {

  if(resp_body_team$success != 1L) {
    return(NULL)

  }

  c(
    resp_body_team$result[c("id", "user_id", "name", "points", "formation", "complete", "valid", "complete_first_time", "salary_cap", "value", "start_round", "activated_at", "firstname", "lastname")],
    resp_body_team$result$lineup[c("captain", "vice_captain")]
  ) |>
    as_tibble() |>
    rename(team_id = id)

}

#' @export
team_user_id <- function(resp_body_team) {
  if(resp_body_team$success != 1L) {
    return(NULL)

  }

  tibble(
    team_id = resp_body_team$result$id,
    user_id = resp_body_team$result$user_id,
    name = resp_body_team$result$name,
    firstname = resp_body_team$result$firstname,
    lastname = resp_body_team$result$lastname
  )

}

get_team_latest_round <- function(resp_body_team) {
  resp_body_team$result$scoreflow |>
    names() |>
    as.integer() |>
    max()
}

#' @export
team_lineup <- function(resp_body_team, team_round = NULL) {
  if(is.null(team_round)) {
    team_round <- get_team_latest_round(resp_body_team)
  }
  captain <- resp_body_team$result$lineup$captain
  vice_captain <- resp_body_team$result$lineup$vice_captain
  if(length(captain) == 0) {
    captain <- -1L
  }
  if(length(vice_captain) == 0) {
    vice_captain <- -1L
  }

  formation <- resp_body_team$result$formation

  utility_position <- case_when(
    formation == "6-8-2-6/3-2-1-2" ~ "Def",
    formation == "6-8-2-6/2-3-1-2" ~ "Mid",
    formation == "6-8-2-6/2-2-2-2" ~ "Ruc",
    formation == "6-8-2-6/2-2-1-3" ~ "Fwd"
    )

  emergencies <- resp_body_team$result$lineup$bench$emergency |>
    unlist()

  field_df <- resp_body_team$result$lineup[c("1", "2", "3", "4")] |>
    clean_team_lineup() |>
    mutate(
      is_bench = FALSE
    )

  bench_df <-  resp_body_team$result$lineup$bench[c("1", "2", "3", "4")] |>
    clean_team_lineup() |>
    mutate(
      is_bench = TRUE
    )

  bind_rows(
    field_df, bench_df
  ) |>
    transmute(
      team_id = resp_body_team$result$id,
      round = as.integer(team_round),
      utility_position = utility_position,
      player_id,
      line_name,
      is_bench,
      is_emergency = player_id %in% emergencies,
      is_captain = player_id == captain,
      is_vice_captain = player_id == vice_captain,
    )

}

clean_team_lineup <- function(lineup) {
  lineup |>
    map2(c("Def", "Mid", "Ruc", "Fwd"), clean_team_lineup_single_line) |>
    list_rbind()
}

clean_team_lineup_single_line <- function(line, line_name) {
  tibble(
    player_id = unlist(line),
    line_name = line_name
  )
}

#' @export
team_scores_by_round <- function(resp_body_team) {
  scoreflow <- resp_body_team$result$scoreflow |>
    unlist()

  tibble(
    team_id = resp_body_team$result$id,
    round = as.integer(names(scoreflow)),
    score = scoreflow
  )
}

#' @export
team_rank <- function(resp_body_rank) {
  resp_body_rank$result[c("id", "firstname", "lastname", "name", "points", "round_rank", "rank", "num_teams")] |>
    as_tibble() |>
    rename(team_id = id)

}

#' @export
team_ranks_by_round <- function(resp_body_rank) {

  scoreflow <- unlist(resp_body_rank$result$scoreflow)
  league_scoreflow <- unlist(resp_body_rank$result$league_scoreflow)
  rank_history <- unlist(resp_body_rank$result$rank_history)
  round_rank_history <- unlist(resp_body_rank$result$round_rank_history)

  rounds <- scoreflow |>
    names() |>
    as.integer()

  tibble(
    team_id = resp_body_rank$result$id,
    round = rounds,
    score = scoreflow,
    league_score = league_scoreflow,
    rank = rank_history,
    round_rank = round_rank_history
  )

}

#' @export
rounds <- function(resp_body_rounds) {
  resp_body_rounds |>
    map(clean_round) |>
    list_rbind() |>
    rename(round = id)

}

#' @export
rounds_matches <- function(resp_body_rounds) {
  resp_body_rounds |>
    map(clean_round_matches) |>
    list_rbind() |>
    rename(match_id = id)
}

#' @export
rounds_locked_squads <- function(resp_body_rounds) {
  resp_body_rounds |>
    map(clean_round_locked_squads) |>
    list_rbind()
}

#' @export
rounds_bye_squads <- function(resp_body_rounds) {
  resp_body_rounds |>
    map(clean_round_bye_squads) |>
    list_rbind()

}

clean_round <- function(round) {
  round[c("id",  "status",  "start",  "end",  "is_bye",  "is_partial_bye",  "is_final",  "lockout",  "saturday_lockout",  "lifted_at")] |>
    as_tibble()
}

clean_round_locked_squads <- function(round) {
  if(length(round$locked) == 0) {
    return(NULL)
  }
  round_num <- round$id
  locked <- unlist(round$locked)
  tibble(
    round = round_num,
    squad_id = locked
  )

}

clean_round_bye_squads <- function(round) {
  if(length(round$bye_squads) == 0) {
    return(NULL)
  }
  round_num <- round$id
  bye_squads <- unlist(round$bye_squads)
  tibble(
    round = round_num,
    squad_id = bye_squads
  )

}

clean_round_matches <- function(round) {
  round$matches |>
    map(clean_round_match) |>
    list_rbind()
}

clean_round_match <- function(round_match) {
  nullable_columns <- c("home_goals", "away_goals", "home_behinds", "away_behinds")

  if("clock" %in% names(round_match)) {
    clock <- list(
      clock_period = round_match$clock$p,
      clock_seconds = round_match$clock$s
    )
  } else {
    clock <- list(
      clock_period = NA_character_,
      clock_seconds = NA_integer_
    )
  }
  c(
    round_match[setdiff(names(round_match), c("clock", nullable_columns))],
    round_match[nullable_columns] |>
      map(~{
        if(is.null(.x)) {
          NA_integer_
        } else {
          .x
        }
      }) |>
      `names<-`(nullable_columns),
    clock
  ) |>
    as_tibble()

}

#' @export
rankings <- function(resp_body_rankings) {
  resp_body_rankings$result |>
    map(clean_ranking) |>
    list_rbind()
}

clean_ranking <- function(ranking) {
  if(is.null(ranking$last_round_points)) {
    last_round_points <- list(last_round_points = NA_integer_)

  } else {
    last_round_points <- list(last_round_points = ranking$last_round_points)
  }
  c(
  ranking[c("team_id", "team_name", "user_id", "firstname", "lastname",  "league_points",  "rank", "value", "salary_cap",  "avatar_version", "this_round_points", "overall_rank",  "highest_round_score")],
  last_round_points
  ) |>
    as_tibble()
}

