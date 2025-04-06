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

next_round <- af_pipelines$current_round() +1

player_stats_2025 <- fetch_player_stats_afl(season = 2025)
player_stats_2024 <- fetch_player_stats_afl(season = 2024)
player_stats_2023 <- fetch_player_stats_afl(season = 2023)
player_stats_2022 <- fetch_player_stats_afl(season = 2022)
player_stats_2021 <- fetch_player_stats_afl(season = 2021)
player_stats_2020 <- fetch_player_stats_afl(season = 2020)
player_stats_2019 <- fetch_player_stats_afl(season = 2019)
player_stats_2018 <- fetch_player_stats_afl(season = 2018)
player_stats_2017 <- fetch_player_stats_afl(season = 2017)
player_stats_2016 <- fetch_player_stats_afl(season = 2016)

teams <- unique(c(player_stats_2025$team.name))
df <- data.frame(team = teams)

unique_players <- unique(c(player_stats_2025$player.playerId, player_stats_2024$player.playerId, player_stats_2023$player.playerId, player_stats_2022$player.playerId, player_stats_2021$player.playerId, player_stats_2020$player.playerId))
df_2 <- data.frame(player = unique_players)

upcoming_fix <- player_stats_2025 |>
  filter(round.roundNumber == next_round) |>
  select(home.team.name, away.team.name)

df <- df |>
  left_join(upcoming_fix, by = c("team" = "home.team.name")) |>
  left_join(upcoming_fix, by = c("team" = "away.team.name")) |>
  mutate(opposition = if_else(is.na(home.team.name), away.team.name, home.team.name)) |>
  filter(!(is.na(team))) |>
  select(-c("home.team.name", "away.team.name"))

df_2 <- df_2 |>
  left_join(player_stats_2025 |> select("player.playerId", "team.name"), by = c("player" = "player.playerId")) |>
  distinct(player, team.name)

df_2 <- df_2 |>
  rename(team = team.name) |>
  left_join(df, by = "team")


player_stats_2025 <- player_stats_2025  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2025) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats_2024 <- player_stats_2024  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2024) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats_2023 <- player_stats_2023  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2023) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats_2022 <- player_stats_2022  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2022) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats_2021 <- player_stats_2021  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2021) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats_2020 <- player_stats_2020  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2020) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)


player_stats_2019 <- player_stats_2019  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2019) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)


player_stats_2018 <- player_stats_2018  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2018) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)


player_stats_2017 <- player_stats_2017  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2017) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)


player_stats_2016 <- player_stats_2016  |>
  mutate(oppo = if_else(team.name == home.team.name, away.team.name, home.team.name)) |>
  mutate(season = 2016) |>
  rowwise() |>
  mutate(af = 3*kicks+2*handballs+4*tackles+3*marks+hitouts+freesFor-3*freesAgainst+6*goals+behinds) |>
  ungroup()|>
  select(player.playerId, team.name, af, home.team.name, away.team.name, status, round.roundNumber, oppo, season)

player_stats <- rbind(player_stats_2025,
                      player_stats_2024,
                      player_stats_2023,
                      player_stats_2022,
                      player_stats_2021,
                      player_stats_2020,
                      player_stats_2019,
                      player_stats_2018,
                      player_stats_2017,
                      player_stats_2016)

player_stats <- player_stats |>
  filter(status == "CONCLUDED")

player_stats_l3 <- player_stats |>
  left_join(df_2 |> select("player", "opposition"),
            by = c("player.playerId" = "player")) |>
  filter(oppo == opposition) |>
  arrange(player.playerId, desc(season), desc(round.roundNumber)) |>
  group_by(player.playerId) |>
  mutate(row_num = row_number()) |>
  ungroup() |>
  filter(row_num <= 3)

player_stats_l1 <- player_stats |>
  left_join(df_2 |> select("player", "opposition"),
            by = c("player.playerId" = "player")) |>
  filter(oppo == opposition) |>
  arrange(player.playerId, desc(season), desc(round.roundNumber)) |>
  group_by(player.playerId) |>
  mutate(row_num = row_number()) |>
  ungroup() |>
  filter(row_num <= 1)

export <- player_stats |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(af),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)

out <- df_2 |>
  rename("player_id" = "player",
         "oppo" = "opposition") |>
  left_join(export, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")



export_l3 <- player_stats_l3 |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(af),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)



out_l3 <- df_2 |>
  rename("player_id" = "player",
         "oppo" = "opposition") |>
  left_join(export_l3, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")



export_l1 <- player_stats_l1 |>
  group_by(player.playerId, oppo) |>
  summarise(total_score = sum(af),
            number_games = n()) |>
  mutate(oppo_avg = total_score/number_games) |>
  select(player.playerId, oppo, oppo_avg) |>
  rename("player_id" = player.playerId)

out_l1 <- df_2 |>
  rename("player_id" = "player",
         "oppo" = "opposition") |>
  left_join(export_l1, by = c("player_id", "oppo")) |>
  select("player_id", "oppo_avg")


fwrite(out, here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_r_",current_round,".csv")))
fwrite(out_l3, here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_l3_r_",current_round,".csv")))
fwrite(out_l1, here("data","exports","2025","_for_mm","zz_adhoc",paste0("oppo_avg_l1_r_",current_round,".csv")))
