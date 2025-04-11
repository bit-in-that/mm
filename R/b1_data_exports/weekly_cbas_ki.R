# For now will just do round 1 but want to also do the full history
# TODO: clean up the way AF and SC scores are looked up (currently very adhoc and including a non-productionised version of the SC api)
box::use(
  fitzRoy[fetch_player_stats_afl, fetch_results_afl],
  here[here],
  dplyr[...],
  httr2[...],
  tidyr[replace_na],
  purrr[...],
  readxl[read_excel]
)

box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines
)

#' @export
cba_ki <- function() {

  season <- 2025
  current_round <- af_pipelines$current_round()

  player_stats <- fetch_player_stats_afl(season = season, round_number = current_round)
  af_players_by_round <- af_pipelines$players_by_round()
  sc_players_stats <- sc_pipelines$players_stats()|>
    filter(round == current_round)
  sc_players_id <- sc_pipelines$players() |>
    transmute(player_id, feed_id, Player = paste(first_name, last_name), Team = team_abbrev)

  adhoc_changes_id <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "player_id")

  # changing nic martin player id
  af_players_by_round <- af_players_by_round |>
    mutate(player_id = paste0("CD_I", player_id)) |>
    left_join(adhoc_changes_id |>
                select(-Name),
              by = "player_id") |>
    mutate(player_id = if_else(is.na(player.playerId), player_id, player.playerId)) |>
    select(-c(player.playerId))

  # getting the af_scores
  af_scores <- af_players_by_round |>
    filter(round == current_round) |>
    transmute(player_id, AF = score)

  sc_players_stats <- sc_players_stats |>
    left_join(sc_players_id,
              by = "player_id") |>
    mutate(player_id = paste0("CD_I",feed_id)) |>
    select(player_id, sc_cost = price, minutes_played, SC = points)

  player_stats <- player_stats |>
    rename(player_id = player.playerId) |>
    mutate(extendedStats.centreBounceAttendances = replace_na(extendedStats.centreBounceAttendances, 0)) |>
    mutate(extendedStats.kickins = replace_na(extendedStats.kickins, 0))

  results_stats <- fetch_results_afl(season = 2025, round = 4)

  results_stats <- results_stats |>
    rowwise() |>
    mutate(home_score = 6*homeTeamScoreChart.goals + sum(homeTeamScoreChart.leftBehinds,
                                                         homeTeamScoreChart.rightBehinds,
                                                         homeTeamScoreChart.leftPosters,
                                                         homeTeamScoreChart.rightPosters,
                                                         homeTeamScoreChart.rushedBehinds,
                                                         homeTeamScoreChart.touchedBehinds)) |>
    mutate(away_score = 6*awayTeamScoreChart.goals + sum(awayTeamScoreChart.leftBehinds,
                                                         awayTeamScoreChart.rightBehinds,
                                                         awayTeamScoreChart.leftPosters,
                                                         awayTeamScoreChart.rightPosters,
                                                         awayTeamScoreChart.rushedBehinds,
                                                         awayTeamScoreChart.touchedBehinds)) |>
    ungroup() |>
    select(match.homeTeam.name, match.awayTeam.name, round.roundNumber, home_score, away_score) |>
    rename(home.team.club.name = match.homeTeam.name,
           away.team.club.name = match.awayTeam.name)

  results_stats_v2 <- results_stats |>
    mutate(home.team.club.name = results_stats$away.team.club.name) |>
    mutate(away.team.club.name = results_stats$home.team.club.name) |>
    mutate(home_score = results_stats$away_score) |>
    mutate(away_score = results_stats$home_score)

  data <- rbind(results_stats, results_stats_v2)

  data <- data |>
    mutate(result = if_else(home_score > away_score, "W",
                            if_else(home_score == away_score, "D", "L"))) |>
    select(team.name = home.team.club.name, result)

  player_stats <- player_stats |>
    left_join(data,
              by = "team.name")

  out_player_stats <- player_stats |>
    group_by(teamId) |>
    mutate(
      TeamCBA = sum(extendedStats.centreBounceAttendances) /4,
      TeamKI = sum(extendedStats.kickins)
    ) |>
    ungroup()  |>
    transmute(
      matchId = providerId,
      player_id,
      TOG = timeOnGroundPercentage,
      CBA = extendedStats.centreBounceAttendances,
      KI = extendedStats.kickins,
      CBA_PERC = round(CBA / TeamCBA * 100, 0),
      KI_PERC = round(KI / TeamKI * 100, 0),
      TeamCBA,
      TeamKI,
      team.name,
      home.team.name,
      away.team.name,
      venue.name,
      result
    )

  mm_cba <-  sc_players_stats |>
    left_join(af_scores, by = "player_id") |>
    left_join(out_player_stats, by = "player_id")

  return(mm_cba)

}
