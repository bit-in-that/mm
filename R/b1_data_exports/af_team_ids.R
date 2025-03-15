box::use(
  ../a1_modules/af_pipelines
  )
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  arrow[write_parquet]
)

load_dot_env() # a .env file doesn't exist, create one with your session ID in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ids <- 1:100 |>
  af_pipelines$team_user_ids(session_id = session_id)

write_parquet(af_team_ids, here("../data/exports/2025", "af_team_ids.parquet"))
