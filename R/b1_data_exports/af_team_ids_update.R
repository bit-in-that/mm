# af_team_ids should be run first then if we would like to update the list, we can run this script (appends onto the data)
box::use(
  ../a1_modules/af_pipelines
)

box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  arrow[read_parquet, write_parquet]
)

load_dot_env() # a .env file doesn't exist, create one with your session ID in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ids_initial <- arrow::read_parquet(here("data/exports/2025", "af_team_ids.parquet"))

team_count <- af_pipelines$team_count(session_id)

existing_teams <- af_team_ids_initial$team_id

potential_missing_teams <- setdiff(seq(team_count + 50L), existing_teams)


af_team_ids_new <- potential_missing_teams |>
  af_pipelines$team_user_ids(session_id = session_id)

af_team_ids <- af_team_ids_initial |>
  bind_rows(af_team_ids_new)

# Stop early if no new teams were added
stopifnot(nrow(af_team_ids) > nrow(af_team_ids_initial))

if(!is.null(af_team_ids)) {
  write_parquet(af_team_ids, here("data/exports/2025", "af_team_ids.parquet"))

}


