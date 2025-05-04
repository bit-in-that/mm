box::use(
  ../a1_modules/af_api,
  ../a1_modules/af_pipelines,
  ../a1_modules/af_tabulate
)

box::reload(af_api)
box::reload(af_tabulate)
box::reload(af_pipelines)

box::use(
  dplyr[...],
  purrr[...]
)

af_pipelines$team_user_ids(session_id = session_id_bit, team_ids = 1:2) |>
  pull(user_id) |>
  af_pipelines$team_ranks(session_id = session_id_bit) |>
  pull(team_id) |>
  af_pipelines$team_lineups(session_id = session_id_bit) |>
  View()

