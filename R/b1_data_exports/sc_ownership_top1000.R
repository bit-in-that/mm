box::use(
  ../a1_modules/sc_pipelines,
  ../a1_modules/af_pipelines
)
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  data.table[fwrite]
)


#' @export
sc_own <- function() {

  load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
  access_token <- Sys.getenv("SC_ACCESS_TOKEN")

  # TODO: remove need for this eventually
  current_round <- af_pipelines$current_round()
  # TODO: remove need for this eventually
  players_af <- af_pipelines$players()
  # TODO: remove need for this eventually
  squads <- af_pipelines$squads()

  rankings <- sc_pipelines$rankings(access_token, pages = 1:10)
  players <- sc_pipelines$players()
  players_stats <- sc_pipelines$players_stats() |>
    filter(round == current_round)

  lineups_top1000 <- rankings |>
    filter(raw_position %in% 1:1000) |>
    pull(team_id) |>
    sc_pipelines$team_lineups(access_token = access_token)

  # Check number of teams is 1000
  lineups_top1000 |>
    pull(user_team_id) |>
    unique() |>
    length()

  # for some reason the first time I ran it, it only got 750 teams, need to get the missing ones
  # TODO: might be because of parrallel processing, can look into this
  lineups_leftover <- rankings |>
    filter(!team_id %in% unique(lineups_top1000$user_team_id)) |>
    pull(team_id) |>
    sc_pipelines$team_lineups(access_token = access_token)

  sc_ownership <- lineups_top1000 |>
    bind_rows(lineups_leftover) |>
    left_join(
      rankings |> select(team_id, raw_position),
      by = c("user_team_id" = "team_id")
    ) |>
    full_join(
      players |> transmute(player_id, feed_id, Player = paste(first_name, last_name), Team = team_abbrev),
      by = "player_id"
    ) |>
    left_join(
      players_stats |> select(player_id, OwnershipTotal = owned),
      by = "player_id"
    ) |>
    group_by(
      feed_id, Player, Team, OwnershipTotal
    ) |>
    summarise(
      OwnershipTop1000 = sum(raw_position <= 1000L, na.rm = TRUE)/10,
      OwnershipTop100 = sum(raw_position <= 100L, na.rm = TRUE),
      OwnershipTop10 = sum(raw_position <= 10L, na.rm = TRUE) * 10,
      .groups = "drop"
    ) |>
    arrange(desc(OwnershipTop1000))

  sc_ownership <- sc_ownership |>
    rename("sc_OwnershipTotal" = "OwnershipTotal",
           "sc_OwnershipTop1000" = "OwnershipTop1000",
           "sc_OwnershipTop100" = "OwnershipTop100",
           "sc_OwnershipTop10" = "OwnershipTop10") |>
    mutate(player_id = paste0("CD_I",feed_id))

  return(sc_ownership)


}




