box::use(
  here[here],
  httr2[...],
  dplyr[...],
  purrr[...],
  stringr[...],
  fitzRoy[...],
  jsonlite[...],
  data.table[fread, fwrite]
)

source("R/zz_adhoc/aflw_fantasy_player_stats_function.R")


current_season <- 2025
current_round_end <- 2

afl_token <- get_afl_cookie()

afw_players <- get_player_data()
afw_players_round <- get_player_data(by_round = TRUE)


competitions <- request("https://aflapi.afl.com.au/afl/v2/competitions?pageSize=50") |>
  req_perform() |>
  resp_body_json() |>
  pluck("competitions") |>
  map(as_tibble) |>
  list_rbind()

competiton_id_aflw <- competitions |>
  filter(code == "AFLW") |>
  pull(id)

seasons_aflw <- paste0("https://aflapi.afl.com.au/afl/v2/competitions/", competiton_id_aflw, "/compseasons?pageSize=100") |>
  request() |>
  req_perform() |>
  resp_body_json() |>
  pluck("compSeasons") |>
  map(as_tibble) |>
  list_rbind()

season_id_aflw_current <- seasons_aflw |>
  filter(str_detect(name, paste0("^", current_season))) |>
  pull(providerId)

season_id_cd <- season_id_aflw_current
season_player_stats <- paste0("https://api.afl.com.au/statspro/playersStats/seasons/", season_id_cd) |>
  request() |>
  req_headers('x-media-mis-token' = afl_token) |>
  req_perform() |>
  resp_body_json() |>
  pluck("players")

season_player_stats |>
  write_json(here("data/exports/aflw/snapshots", paste0("aflw_player_details_r", current_round_end, "_", current_season, ".json")))


player_details <- season_player_stats |>
  map(~{
    tibble(
      playerId = .x$playerId,
      gamesPlayed = x$gamesPlayed
    ) |>
      bind_cols(.x$team) |>
      bind_cols(.x$playerDetails)
  }) |>
  list_rbind() |>
  mutate(
    snapshot_season = 2025,
    snapshot_round = current_round_end
  ) |>
  relocate(snapshot_season, snapshot_round, .after = playerId)


season_averages <- season_player_stats |>
  map(~{
    tibble(
      playerId = .x$playerId,
      givenName = .x$playerDetails$givenName,
      surname = .x$playerDetails$surname,
      gamesPlayed = x$gamesPlayed
    ) |>
      bind_cols(.x$team) |>
      bind_cols(.x$averages)
  }) |>
  list_rbind() |>
  mutate(
    snapshot_season = 2025,
    snapshot_round = current_round_end
  ) |>
  relocate(snapshot_season, snapshot_round, .after = playerId)

season_totals <- season_player_stats |>
  map(~{
    tibble(
      playerId = .x$playerId,
      givenName = .x$playerDetails$givenName,
      surname = .x$playerDetails$surname,
      gamesPlayed = x$gamesPlayed
    ) |>
      bind_cols(.x$team) |>
      bind_cols(.x$totals)
  }) |>
  list_rbind() |>
  mutate(
    snapshot_season = 2025,
    snapshot_round = current_round_end
  ) |>
  relocate(snapshot_season, snapshot_round, .after = playerId)

season_averages |>
  fwrite(here("data/exports/aflw/snapshots", paste0("aflw_player_season_averages_r", current_round_end, "_", current_season, ".csv")))

season_totals |>
  fwrite(here("data/exports/aflw/snapshots", paste0("aflw_player_season_totals_r", current_round_end, "_", current_season, ".csv")))

player_details |>
  fwrite(here("data/exports/aflw/snapshots", paste0("aflw_player_details_r", current_round_end, "_", current_season, ".csv")))


# TODO: use stacking to make this process work for future rounds
season_totals_prior <- fread(here("data/exports/aflw/snapshots", paste0("aflw_player_season_totals_r", current_round_end - 1, "_", current_season, ".csv")))

team_stats_prior <- season_totals_prior |>
  group_by(teamId) |>
  summarise(
    cba_team_prior = sum(centreBounceAttendances, na.rm = TRUE) / 4,
    ki_team_prior = sum(kickins, na.rm = TRUE),
    ki_po_team_prior = sum(kickinsPlayon, na.rm = TRUE),
    rc_team_prior = sum(ruckContests, na.rm = TRUE),
    .groups = "drop"
  )


hard_to_get_stats <- season_totals |>
  left_join(team_stats_prior, by = "teamId") |>
  group_by(teamId) |>
  mutate(
    cba_team = sum(centreBounceAttendances, na.rm = TRUE) / 4 - cba_team_prior,
    ki_team = sum(kickins, na.rm = TRUE) - ki_team_prior,
    ki_po_team = sum(kickinsPlayon, na.rm = TRUE) - ki_po_team_prior,
    rc_team = sum(ruckContests, na.rm = TRUE) - rc_team_prior
  ) |>
  ungroup() |>
  select(-starts_with("team_prior")) |>
  transmute(
    playerId,
    teamId,
    snapshot_season,
    snapshot_round,
    player_name = paste(givenName, surname),
    teamNickname ,
    cba = centreBounceAttendances,
    cba_prop = cba/cba_team * 100,
    cba_team,
    ki = kickins,
    ki_team,
    ki_po_team,
    ki_po = kickinsPlayon,
    ki_po_prop = if_else(ki_po == 0L, NA_real_, ki / ki_po * 100),
    ki_prop = if_else(ki_team == 0L, NA_real_, ki / ki_team * 100),
    rc = ruckContests,
    rc_team,
    rc_prop = rc / rc_team * 100
  ) |>
  left_join(
    afw_players |> transmute(playerId = paste0("CD_I", id), position), by = "playerId"
  ) |>
  relocate(position, .after = player_name)

team_stats <- hard_to_get_stats |>
  distinct(teamId, teamNickname, cba_team, ki_team, ki_po_team, rc_team) |>
  mutate(snapshot_round = current_round_end) |>
  relocate(snapshot_round, .before = "teamId")

hard_to_get_stats |>
  fwrite(here("data/exports/aflw/snapshots", paste0("aflw_player_summary_r", current_round_end, "_", current_season, ".csv")))

team_stats |>
  fwrite(here("data/exports/aflw/snapshots", paste0("aflw_team_stats_r", current_round_end, "_", current_season, ".csv")))

