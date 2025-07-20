box::use(
  here[here],
  httr2[...],
  dplyr[...],
  purrr[...],
  stringr[...],
  fitzRoy[...]
)


current_season <- 2025
previous_season <- current_season - 1
prior_season <- current_season - 2

# for PPM:
total_minutes_per_game <- 80 # this is an approximation (there is an API to get actual minutes elapsed in each game but just using this approximation for now)


afl_token <- get_afl_cookie()


competitions <- request("https://aflapi.afl.com.au/afl/v2/competitions?pageSize=50") |>
  req_perform() |>
  resp_body_json() |>
  pluck("competitions") |>
  map(as_tibble) |>
  list_rbind()

competiton_id_aflw <- competitions |>
  filter(code == "AFLW") |>
  pull(id)

competiton_id_u18g <- competitions |>
  filter(name == "Coates Talent League Girls") |>
  pull(id)

competiton_id_u18g_state <- competitions |>
  filter(code == "U18CG") |>
  pull(id)

competiton_id_vflw <- competitions |>
  filter(code == "VFLW") |>
  pull(id)


seasons_vflw <- paste0("https://aflapi.afl.com.au/afl/v2/competitions/", competiton_id_vflw, "/compseasons?pageSize=100") |>
  request() |>
  req_perform() |>
  resp_body_json() |>
  pluck("compSeasons") |>
  map(as_tibble) |>
  list_rbind()


seasons_u18g <- paste0("https://aflapi.afl.com.au/afl/v2/competitions/", competiton_id_u18g, "/compseasons?pageSize=100") |>
  request() |>
  req_perform() |>
  resp_body_json() |>
  pluck("compSeasons") |>
  map(as_tibble) |>
  list_rbind()



seasons_u18g_state <- paste0("https://aflapi.afl.com.au/afl/v2/competitions/", competiton_id_u18g_state, "/compseasons?pageSize=100") |>
  request() |>
  req_perform() |>
  resp_body_json() |>
  pluck("compSeasons") |>
  map(as_tibble) |>
  list_rbind()


seasons_aflw <- paste0("https://aflapi.afl.com.au/afl/v2/competitions/", competiton_id_aflw, "/compseasons?pageSize=100") |>
  request() |>
  req_perform() |>
  resp_body_json() |>
  pluck("compSeasons") |>
  map(as_tibble) |>
  list_rbind()

season_id_aflw_previous <- seasons_aflw |>
  filter(str_detect(name, paste0("^", previous_season))) |>
  pull(providerId)

season_id_aflw_prior <- seasons_aflw |>
  filter(str_detect(name, paste0("^", prior_season))) |>
  pull(providerId)


season_id_u18g <- seasons_u18g |>
  filter(str_detect(name, paste0("^", previous_season))) |>
  pull(providerId)

season_id_u18g_state <- seasons_u18g_state |>
  filter(str_detect(name, paste0("^", previous_season))) |>
  pull(providerId)

season_id_vflw <- seasons_vflw |>
  filter(str_detect(name, paste0("^", current_season))) |>
  pull(providerId)


calculate_fantasy_points <- function(kicks, handballs, marks, tackles, hitouts, freesFor, freesAgainst, goals, behinds) {
  kicks * 3 + handballs * 2 + marks * 3 + tackles * 4 + hitouts * 1 + freesFor * 1 - freesAgainst * 3 + goals * 6 + behinds * 1
}


get_player_season_stats <- function(season_id_cd, type = c("Current", "Prior", "State"), .afl_token = afl_token) {
  type <- type[1]

  season_player_stats <- paste0("https://api.afl.com.au/statspro/playersStats/seasons/", season_id_cd) |>
    request() |>
    req_headers('x-media-mis-token' = .afl_token) |>
    req_perform() |>
    resp_body_json() |>
    pluck("players")

  if(type == "Current") {
    season_player_stats <- season_player_stats |>
      map(~{
        tibble(
          playerId_CD = .x$playerId,
          seasonAvg =  .x$averages$dreamTeamPoints,
          games = .x$gamesPlayed,
          team_id = .x$team$teamId, # need to calculate team totals
          total_cba = .x$totals$centreBounceAttendances,
          total_ki = .x$totals$kickins,
          total_ki_po = .x$totals$kickinsPlayon,
          total_rc = .x$totals$ruckContests,
          tog = .x$averages$timeOnGroundPercentage,
          kicks = .x$averages$kicks,
          handballs = .x$averages$handballs,
          marks = .x$averages$marks,
          tackles = .x$averages$tackles,
          hitouts = .x$averages$hitouts,
          freeFor = .x$averages$freesFor,
          freeAgainst = .x$averages$freesAgainst,
          goals = .x$averages$goals,
          behinds = .x$averages$behinds
        )
      })

  } else if(type == "Prior"){
    season_player_stats <- season_player_stats |>
      map(~{
        tibble(
          playerId_CD = .x$playerId,
          priorSeason =  .x$averages$dreamTeamPoints,
          priorSeasonGames = .x$gamesPlayed
        )
      })
  } else if(type == "State") {
    season_player_stats <- season_player_stats |>
      map(~{
        tibble(
          playerId_CD = .x$playerId,
          stateAvg =  calculate_fantasy_points(.x$averages$kicks, .x$averages$handballs, .x$averages$marks, .x$averages$tackles, .x$averages$hitouts, .x$averages$freesFor, .x$averages$freesAgainst, .x$averages$goals, .x$averages$behinds),
          stateGames = .x$gamesPlayed
        )
      })

  } else {
    stop("Type missing")
  }

  season_player_stats |>
    list_rbind()


}

season_stats_aflw_previous <- get_player_season_stats(season_id_aflw_previous, type = "Current")
season_stats_aflw_prior <- get_player_season_stats(season_id_aflw_prior, type = "Prior")
season_stats_vflw <- get_player_season_stats(season_id_vflw, type = "State")
season_stats_u18g <- get_player_season_stats(season_id_u18g, type = "State") |> rename(u18Avg = stateAvg, u18Games = stateGames)
season_stats_u18g_state <- get_player_season_stats(season_id_u18g_state, type = "State") |> rename(u18StateAvg = stateAvg, u18StateGames = stateGames)


season_stats_aflw_previous <- season_stats_aflw_previous |>
  group_by(team_id) |>
  mutate(
    total_cba_team = sum(total_cba, na.rm = TRUE),
    total_ki_team = sum(total_ki, na.rm = TRUE),
    total_rc_team = sum(total_rc, na.rm = TRUE),
  ) |>
  ungroup() |>
  mutate(
    cba = total_cba / total_cba_team * 100 * 4,
    ki = total_ki / total_ki_team * 100,
    rc = total_rc / total_rc_team * 100,
    ki_po_perc = total_ki_po / total_ki * 100,
    ppm = seasonAvg / tog /  total_minutes_per_game * 100
  )

aflw_player_stats_by_round <- fetch_player_stats_afl(season = previous_season, comp = "AFLW")

aflw_average_summary <- aflw_player_stats_by_round |>
  arrange(utcStartTime) |>
  mutate(
    playerId_CD = player.playerId,
    is_first_half = round.roundNumber %in% 1:5,
    is_second_half = round.roundNumber %in% 6:10,
    is_finals = round.roundNumber > 10,
    ) |>
  group_by(playerId_CD) |>
  summarise(
    ave_points_calc = mean(dreamTeamPoints, na.rm = TRUE),
    gamesPlayed = n(),
    seasonHigh = max(dreamTeamPoints, na.rm = TRUE),
    seasonLow = min(dreamTeamPoints, na.rm = TRUE),
    firstHalf = mean(if_else(is_first_half, dreamTeamPoints, NA_real_), na.rm = TRUE),
    secondHalf = mean(if_else(is_second_half, dreamTeamPoints, NA_real_), na.rm = TRUE),
    lastThree = mean(tail(dreamTeamPoints, 3), na.rm = TRUE),
    lastFive = mean(tail(dreamTeamPoints, 5), na.rm = TRUE),
    finals = mean(if_else(is_finals, dreamTeamPoints, NA_real_), na.rm = TRUE)
  )


aflw_player_details_2025 <- fetch_player_details(season = current_season, comp = "AFLW")

aflw_player_stats <- aflw_player_details_2025 |>
  transmute(
    playerId_CD = providerId,
    name = paste(firstName, surname),
    season,
    dateOfBirth = dateOfBirth,
    team,
    mmRank = NA_integer_
  ) |>
  mutate(
    playerId = str_remove(playerId_CD, "^CD_I")
  )

aflw_player_stats <- aflw_player_stats |>
  left_join(season_stats_aflw_previous, by = "playerId_CD") |>
  left_join(aflw_average_summary, by = "playerId_CD") |>
  left_join(season_stats_aflw_prior, by = "playerId_CD") |>
  left_join(season_stats_vflw, by = "playerId_CD") |>
  left_join(season_stats_u18g, by = "playerId_CD") |>
  left_join(season_stats_u18g_state, by = "playerId_CD")

aflw_player_stats |>
  select(
    playerId, name, seasonAvg, games, cba, ki, tog, ppm, season,
    kicks, handballs, marks, tackles, hitouts, freeFor, freeAgainst, goals, behinds,
    seasonHigh, seasonLow, firstHalf, secondHalf, lastFive, lastThree,
    stateAvg, priorSeason, priorSeasonGames,
    mmRank,	dateOfBirth
  ) |>
  fwrite(here("data/exports/aflw", paste0("aflw_player_hist_", current_season, ".csv")))


aflw_player_stats |>
  select(
    playerId, name, team, seasonAvg, games, cba, ki, ki_po_perc, rc, tog, ppm, season,
    kicks, handballs, marks, tackles, hitouts, freeFor, freeAgainst, goals, behinds,
    seasonHigh, seasonLow, firstHalf, secondHalf, finals, lastFive, lastThree,
    stateAvg, stateGames, priorSeason, priorSeasonGames, u18Avg, u18Games, u18StateAvg, u18StateGames,
    mmRank,	dateOfBirth
  ) |>
  fwrite(here("data/exports/aflw", paste0("aflw_player_hist_", current_season, "_alt.csv")))



# TODO: create an alt version with other information team name, ruck contests, finals average, u18 avg, state comp avg

