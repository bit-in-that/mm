# For now will just do round 1 but want to also do the full history
# TODO: clean up the way AF and SC scores are looked up (currently very adhoc and including a non-productionised version of the SC api)
box::use(
  fitzRoy[fetch_player_stats_afl],
  dplyr[...],
  purrr[...],
  stringr[str_sub],
  httr2[...],
  data.table[fwrite],
  here[here]
)

box::use(
  ../a1_modules/af_pipelines
)


player_stats <- fetch_player_stats_afl(season = 2025, round_number = 1)

af_players_by_round <- af_pipelines$players_by_round()
af_players <- af_pipelines$players()

af_r1_scores <- af_players_by_round |>
  filter(round == 1L) |>
  transmute(
    player.playerId = paste0("CD_I", player_id),
    AF = score
  ) |>
  mutate(
    # Manually fix up Nic Martin (for some reason his player ID is wrong in the afl fantasy api)
    player.playerId = if_else(player.playerId == "CD_I1026850", "CD_I1012013", player.playerId)
  )

sc_players <- request("https://www.supercoach.com.au/2025/api/afl/classic/v1/players-cf?embed=notes%2Codds%2Cplayer_stats%2Cpositions") |>
  req_perform() |>
  resp_body_json()
#
# sc_players <- request("https://www.supercoach.com.au/2024/api/afl/classic/v1/players-cf?embed=notes%2Codds%2Cplayer_stats%2Cpositions") |>
#   req_perform() |>
#   resp_body_json()

sc_r1_scores <- sc_players |>
  map(~{
    tibble(
      player_id_sc = .x$id,
      player_id = .x$feed_id,
      SC = .x$player_stats[[1]]$points
    )
  }) |>
  bind_rows() |>
  transmute(
    player.playerId = paste0("CD_I", player_id),
    SC
  )


mm_cba <- player_stats |>
  group_by(teamId) |>
  mutate(
    TeamCBA = sum(extendedStats.centreBounceAttendances) /4,
    TeamKI = sum(extendedStats.kickins)
    ) |>
  ungroup() |>
  left_join(af_r1_scores, by = "player.playerId") |>
  left_join(sc_r1_scores, by = "player.playerId") |>
  transmute(
    Season = str_sub(utcStartTime, start = 1, end = 4) |>
      as.integer(),
    roundNumber = round.roundNumber,
    matchId = providerId,
    playerId = player.playerId,
    name = paste(player.givenName, player.surname),
    TOG = timeOnGroundPercentage,
    AF, #note that dreamTeamPoints sometimes disagrees with the official scores in the actual game so have to use the AF version instead
    CBA = extendedStats.centreBounceAttendances,
    KI = extendedStats.kickins,
    teamName = team.name,
    CBA_PERC = round(CBA / TeamCBA * 100, 0),
    KI_PERC = round(KI / TeamKI * 100, 0),
    SC,
    TeamCBA,
    TeamKI
  )


fwrite(mm_cba, here("data/exports/2025/_for_mm", "cba_r1.csv"))

