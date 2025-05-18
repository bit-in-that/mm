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
cba_ki <- function(current_round, season) {

  squads <- af_pipelines$squads(apply_adhoc_changes = TRUE)
  squads_afl <- squads |>
    mutate(
      afl_id = paste0("CD_T", squad_id)
    )

  player_stats <- fetch_player_stats_afl(season = season, round_number = current_round)
  af_players_by_round <- af_pipelines$players_by_round()
  sc_players_stats <- sc_pipelines$players_stats(round = current_round)
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

  results_stats <- fetch_results_afl(season = season, round = current_round)

  results_stats <- results_stats |>
    left_join(
      squads_afl |> select(match.homeTeamId = afl_id, home.team.club.name = full_name),
      by = "match.homeTeamId"
      ) |>
    left_join(
      squads_afl |> select(match.awayTeamId = afl_id, away.team.club.name = full_name),
      by = "match.awayTeamId"
    )

  results_stats <- results_stats |>
    mutate(home_score = 6 * homeTeamScoreChart.goals
           + homeTeamScoreChart.leftBehinds
           + homeTeamScoreChart.rightBehinds
           + homeTeamScoreChart.leftPosters
           + homeTeamScoreChart.rightPosters
           + homeTeamScoreChart.rushedBehinds
           + homeTeamScoreChart.touchedBehinds
           ) |>
    mutate(away_score = 6 * awayTeamScoreChart.goals
           + awayTeamScoreChart.leftBehinds
           + awayTeamScoreChart.rightBehinds
           + awayTeamScoreChart.leftPosters
           + awayTeamScoreChart.rightPosters
           + awayTeamScoreChart.rushedBehinds
           + awayTeamScoreChart.touchedBehinds) |>
    select(match.homeTeamId, match.awayTeamId, home.team.club.name, away.team.club.name, round.roundNumber, home_score, away_score)

  team_result_data <- bind_rows(
    results_stats |>
      transmute(teamId = match.homeTeamId,
                team.club.name = home.team.club.name, Opposition = away.team.club.name,
                score = home_score, score_opp = away_score,
                home_status = "home"
                ),
    results_stats |>
      transmute(teamId = match.awayTeamId,
                team.club.name = away.team.club.name, Opposition = home.team.club.name,
                score = away_score, score_opp = home_score,
                home_status = "away"
                )
  ) |>
    mutate(result = case_when(
      score == score_opp ~ "D",
      score > score_opp ~ "W",
      TRUE ~ "L"
      )
    ) |>
    select(teamId, team.club.name, Opposition, home_status, result)


  player_stats2 <- player_stats |>
    left_join(team_result_data,
              by = "teamId")

  out_player_stats <- player_stats2 |>
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
      team.name = team.club.name,
      Opposition,
      home_status,
      # home.team.name = home.team.club.name,
      # away.team.name = away.team.club.name,
      venue.name,
      result
    )

  mm_cba <-  sc_players_stats |>
    left_join(af_scores, by = "player_id") |>
    left_join(out_player_stats, by = "player_id")

  return(mm_cba)

}
