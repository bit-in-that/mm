box::use(
  ../a1_modules/af_pipelines
)
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  readxl[read_excel],
  data.table[fwrite]
)

#' @export
af_own <- function() {

  load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
  session_id <- Sys.getenv("AF_SESSION_ID")

  current_round <- af_pipelines$current_round()
  rankings <- af_pipelines$rankings(session_id, order = "rank", order_direction = "ASC")
  players <- af_pipelines$players()
  squads <- af_pipelines$squads()

  adhoc_changes_id <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "player_id")


  lineups_top1000 <- rankings |>
    filter(rank %in% 1:1000) |>
    pull(team_id) |>
    af_pipelines$team_lineups(session_id = session_id)

  af_ownership <- lineups_top1000 |>
    left_join(
      rankings |> select(team_id, overall_rank),
      by = "team_id"
    ) |>
    full_join(
      players |> transmute(player_id, Player = paste(first_name, last_name), OwnershipTotal = owned_by, squad_id),
      by = "player_id"
    ) |>
    left_join(
      squads |> select(squad_id, Team = short_name),
      by = "squad_id"
    ) |>
    group_by(
      player_id, Player, Team, OwnershipTotal
    ) |>
    summarise(
      OwnershipTop1000 = sum(overall_rank <= 1000L, na.rm = TRUE)/10,
      OwnershipTop100 = sum(overall_rank <= 100L, na.rm = TRUE),
      OwnershipTop10 = sum(overall_rank <= 10L, na.rm = TRUE) * 10,
      .groups = "drop"
    ) |>
    arrange(desc(OwnershipTop1000))

  af_ownership <- af_ownership |>
    rename("af_OwnershipTotal" = "OwnershipTotal",
           "af_OwnershipTop1000" = "OwnershipTop1000",
           "af_OwnershipTop100" = "OwnershipTop100",
           "af_OwnershipTop10" = "OwnershipTop10") |>
    mutate(player_id = paste0("CD_I",player_id))

  # fix up nic martin ID:
  af_ownership <- af_ownership |>
    left_join(
      adhoc_changes_id |> select(player_id, player_id_new = player.playerId),
      by = "player_id"
    ) |>
    mutate(
      player_id = coalesce(player_id_new, player_id)
    ) |>
    select(-player_id_new)

  return(af_ownership)

}
