box::use(
  ../a1_modules/af_pipelines
)

box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  arrow[write_parquet, read_parquet]
)

resp_body_rounds <- af_api$get_rounds()




load_dot_env() # a .env file doesn't exist, create one with your session ID in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

af_team_ids <- arrow::read_parquet(here("data/exports/2025", "af_team_ids.parquet"))

# TODO: complete this code using the user_ids dataset and the other code

