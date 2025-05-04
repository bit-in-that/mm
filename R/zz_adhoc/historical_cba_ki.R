box::use(
  ../a1_modules/af_pipelines
)
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  data.table[fwrite],
  jsonlite[write_json],
  stringr[str_sub],
  arrow[write_parquet, read_parquet],
  tidyr[replace_na]
)


player_stats_afl_2012_to_2024 <- 2021:2025 |> map(fetch_player_stats_afl) |> list_rbind()


# MT note to come back to the NA fix and see if this is actually a good approach
# at a first thought I think it is alright, in that an na would just imply nothing that happened


# notes: need to deal with NA
# |>
#   mutate(across(everything(), ~replace_na(., 0)))

# nic martin need to work this one out

# note this is missing the AF and SC values

export <- player_stats_afl_2012_to_2024 |>
  filter(status == "CONCLUDED") |>
  mutate(Season = as.numeric(str_sub(utcStartTime,1,4))) |>
  rename("playerId" = "player.player.player.playerId",
         "roundNumber" = "round.roundNumber",
         "CBA" = "extendedStats.centreBounceAttendances",
         "KI" = "extendedStats.kickins",
         "matchId" = "providerId",
         "TOG" = "timeOnGroundPercentage",
         "teamName" = "team.name") |>
  mutate(CBA = if_else(is.na(CBA),0,CBA)) |>
  mutate(KI = if_else(is.na(KI),0,KI)) |>
  group_by(Season, roundNumber, teamId) |>
  mutate(
    TeamCBA = sum(CBA) /4,
    TeamKI = sum(KI)
  ) |>
  ungroup() |>
  mutate(CBA_PERC = round(CBA / TeamCBA * 100, 0)) |>
  mutate(KI_PERC = round(KI / TeamKI * 100, 0)) |>
  mutate(name = paste(player.givenName, player.surname)) |>
  filter(Season >= 2021) |>
  select("Season", "roundNumber", "matchId", "playerId", "name", "TOG", "CBA", "KI", "teamName", "CBA_PERC", "KI_PERC", "TeamCBA", "TeamKI") |>
  mutate(teamName = if_else(teamName == "Adelaide Crows", "Adelaide",
                            if_else(teamName == "Footscray", "Western Bulldogs",
                                    if_else(teamName == "Gold Coast SUNS", "Gold Coast Suns",
                                            if_else(teamName == "GWS GIANTS", "GWS Giants", teamName)))))

fwrite(export, here("data","exports","2025","_for_mm","zz_adhoc",paste0("2021_2025_r_5_cba_ki.csv")))
