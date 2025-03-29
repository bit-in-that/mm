# For now will just do round 1 but want to also do the full history
# TODO: clean up the way AF and SC scores are looked up (currently very adhoc and including a non-productionised version of the SC api)
box::use(
  fitzRoy[fetch_player_stats_afl, fetch_results_afl],
  dplyr[...],
  purrr[...],
  stringr[str_sub],
  httr2[...],
  data.table[fwrite],
  here[here]
)

box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines
)

current_round <- af_pipelines$current_round()
player_stats <- fetch_player_stats_afl(season = 2025)
results_stats <- fetch_results_afl(season = 2025)

af_players_by_round <- af_pipelines$players_by_round()


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

af_scores <- af_players_by_round |>
  filter(round == current_round) |>
  transmute(
    player.playerId = paste0("CD_I", player_id),
    AF = score
  ) |>
  mutate(
    # Manually fix up Nic Martin (for some reason his player ID is wrong in the afl fantasy api)
    player.playerId = if_else(player.playerId == "CD_I1026850", "CD_I1012013", player.playerId)
  )


# af_players_by_round <- af_pipelines$players_by_round()
# af_players <- af_pipelines$players()

player_stats <- player_stats |>
  filter(status == "CONCLUDED" & round.roundNumber < current_round)

player_stats_results <- player_stats |>
  left_join(
    results_stats, by = c("home.team.club.name", "away.team.club.name", "round.roundNumber")
  ) |>
  rowwise() |>
  mutate(result = ifelse(teamStatus == "home",ifelse(home_score > away_score,"W","L"),ifelse(home_score > away_score,"L","W"))) |>
  ungroup() |>
  select(player.playerId, round.roundNumber, result, teamStatus) |>
  rename(round = round.roundNumber)


results_avg <- af_players_by_round |>
  mutate(player.playerId = paste0("CD_I", player_id)) |>
  mutate(player.playerId = if_else(player.playerId == "CD_I1026850", "CD_I1012013", player.playerId)) |>
  left_join(
    player_stats_results,
    by = c("player.playerId", "round")
  ) |>
  filter(round < current_round) |>
  group_by(player_id, result) |>
  summarise(total_score = sum(score),
            number_games = n()) |>
  mutate(avg = total_score/number_games)


results_avg_status <- af_players_by_round |>
  mutate(player.playerId = paste0("CD_I", player_id)) |>
  mutate(player.playerId = if_else(player.playerId == "CD_I1026850", "CD_I1012013", player.playerId)) |>
  left_join(
    player_stats_results,
    by = c("player.playerId", "round")
  ) |>
  filter(round < current_round) |>
  group_by(player_id, teamStatus) |>
  summarise(total_score = sum(score),
            number_games = n()) |>
  mutate(avg = total_score/number_games)


results_avg_win <- results_avg |>
  filter(result == "W") |>
  rename(WinAvg = avg) |>
  select(c("player_id", "WinAvg"))

results_avg_loss <- results_avg |>
  filter(result == "L") |>
  rename(LossAvg = avg) |>
  select(c("player_id", "LossAvg"))

results_avg_home <- results_avg_status |>
  filter(teamStatus == "home") |>
  rename(HomeAvg = avg) |>
  select(c("player_id", "HomeAvg"))

results_avg_away <- results_avg_status |>
  filter(teamStatus == "away") |>
  rename(AwayAvg = avg) |>
  select(c("player_id", "AwayAvg"))

player_stats_l3 <- player_stats |>
  arrange(player.playerId, desc(round.roundNumber)) |>
  group_by(player.playerId) |>
  mutate(row_num = row_number()) |>
  ungroup() |>
  filter(row_num <= 3)


# season averages
team_stats <- player_stats |>
  group_by(teamId) |>
  summarise(
    TeamCBA = sum(extendedStats.centreBounceAttendances) / 4,
    TeamKI = sum(extendedStats.kickins)
  )

# Compute player-level statistics and merge team stats
seasonplayer_stats <- player_stats |>
  group_by(player.playerId, teamId) |>
  summarise(
    season_cba_personal = sum(extendedStats.centreBounceAttendances),
    season_ki_personal = sum(extendedStats.kickins),
    season_tog = mean(timeOnGroundPercentage),
    .groups = "drop"
  ) |>
  left_join(team_stats, by = "teamId") |>  # Merge team-level stats
  mutate(
    season_cba = season_cba_personal / TeamCBA,
    season_ki = season_ki_personal / TeamKI
  ) |>
  select(player.playerId, season_cba, season_ki, season_tog) |>
  rename("player_id" = "player.playerId")



# last 3 avg's
team_stats_l3 <- player_stats_l3 |>
  group_by(teamId) |>
  summarise(
    TeamCBA = sum(extendedStats.centreBounceAttendances) / 4,
    TeamKI = sum(extendedStats.kickins)
  )

# Compute player-level statistics and merge team stats
seasonplayer_stats_l3 <- player_stats_l3 |>
  group_by(player.playerId, teamId) |>
  summarise(
    season_cba_personal = sum(extendedStats.centreBounceAttendances),
    season_ki_personal = sum(extendedStats.kickins),
    season_tog = mean(timeOnGroundPercentage),
    .groups = "drop"
  ) |>
  left_join(team_stats_l3, by = "teamId") |>  # Merge team-level stats
  mutate(
    season_cba = season_cba_personal / TeamCBA,
    season_ki = season_ki_personal / TeamKI
  ) |>
  select(player.playerId, season_cba, season_ki, season_tog) |>
  rename("player_id" = "player.playerId")



fwrite(seasonplayer_stats, here("data","exports","2025","_for_mm","season",paste0("season_avg_cba_ki_r",current_round,".csv")))
fwrite(seasonplayer_stats_l3, here("data","exports","2025","_for_mm","last_3",paste0("season_avg_cba_ki_r",current_round,".csv")))
fwrite(results_avg_win, here("data","exports","2025","_for_mm","last_3",paste0("results_avg_win",current_round,".csv")))
fwrite(results_avg_loss, here("data","exports","2025","_for_mm","last_3",paste0("results_avg_loss",current_round,".csv")))
fwrite(results_avg_home, here("data","exports","2025","_for_mm","last_3",paste0("results_avg_home",current_round,".csv")))
fwrite(results_avg_away, here("data","exports","2025","_for_mm","last_3",paste0("results_avg_away",current_round,".csv")))

