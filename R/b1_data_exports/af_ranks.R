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

# TODO: complete this code using the user_ids dataset and the other code
