box::use(
  dplyr[...]
)

box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/af_tabulate
)


players_df <- af_pipelines$players()
players_by_round_df <- af_pipelines$players_by_round()

r0_scores <- players_by_round_df |>
  filter(round == 0) |>
  select(id = player_id, r0_score = score)



players_df |>
  select(id, first_name, last_name, cost, position, break_even, proj_score) |>
  left_join(r0_scores, by = "id") |>
  View()

players_df |>
  transmute(player_id = id, name = paste(first_name, last_name)) |>
  filter(name == "Joel Freijah") |>
  left_join(players_by_round_df, by = "player_id")

