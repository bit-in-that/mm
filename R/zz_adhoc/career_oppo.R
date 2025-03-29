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

# next_round <- af_pipelines$current_round() +1
next_round <- 3

player_stats_2025 <- fetch_player_stats_afl(season = 2025)
player_stats_2024 <- fetch_player_stats_afl(season = 2024)
player_stats_2023 <- fetch_player_stats_afl(season = 2023)
player_stats_2022 <- fetch_player_stats_afl(season = 2022)
player_stats_2021 <- fetch_player_stats_afl(season = 2021)
player_stats_2020 <- fetch_player_stats_afl(season = 2020)

upcoming_fix <- player_stats_2025 |>
  filter(round.roundNumber == next_round)

player_stats_2025 <- player_stats_2025 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2025)

player_stats_2024 <- player_stats_2024 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2024)

player_stats_2023 <- player_stats_2023 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2023)

player_stats_2022 <- player_stats_2022 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2022)

player_stats_2021 <- player_stats_2021 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2021)

player_stats_2020 <- player_stats_2020 |>
  select(player.playerId, team.name, dreamTeamPoints, home.team.name, away.team.name, status, round.roundNumber) |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2020) |>
  mutate(dreamTeamPoints = 1.25*dreamTeamPoints)


player_stats <- rbind(player_stats_2025,
                      player_stats_2024,
                      player_stats_2023,
                      player_stats_2022,
                      player_stats_2021,
                      player_stats_2020)

player_stats_l3 <- player_stats |>
  arrange(player.playerId, season, desc(round.roundNumber)) |>
  group_by(player.playerId) |>
  mutate(row_num = row_number()) |>
  ungroup() |>
  filter(row_num <= 3)

player_stats_l1 <- player_stats |>
  arrange(player.playerId, season, desc(round.roundNumber)) |>
  group_by(player.playerId) |>
  mutate(row_num = row_number()) |>
  ungroup() |>
  filter(row_num <= 1)

export <- player_stats |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(dreamTeamPoints),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)

out <- upcoming_fix |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  rename("player_id" = player.playerId) |>
  left_join(export, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")


export_l3 <- player_stats_l3 |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(dreamTeamPoints),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)

out_l3 <- upcoming_fix |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  rename("player_id" = player.playerId) |>
  left_join(export_l3, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")


export_l1 <- player_stats_l1 |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(dreamTeamPoints),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)

out_l1 <- upcoming_fix |>
  mutate(oppo = ifelse(team.name == home.team.name, away.team.name, home.team.name)) |>
  rename("player_id" = player.playerId) |>
  left_join(export_l1, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")


fwrite(out, here("data","exports","2025","_for_mm","season",paste0("oppo_avg_r",current_round,".csv")))
fwrite(out_l3, here("data","exports","2025","_for_mm","last_3",paste0("oppo_avg_l3_r",current_round,".csv")))
fwrite(out_l1, here("data","exports","2025","_for_mm","last_3",paste0("oppo_avg_l1_r",current_round,".csv")))
