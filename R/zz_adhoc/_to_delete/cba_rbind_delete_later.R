box::use(
  ../a1_modules/af_pipelines,
  ../a1_modules/sc_pipelines
)
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  data.table[fwrite],
  jsonlite[write_json],
  arrow[write_parquet, read_parquet]
)

load_dot_env() # a .env file doesn't exist, create one with your session ID ("AF_SESSION_ID") in the R folder
session_id <- Sys.getenv("AF_SESSION_ID")

current_round <- af_pipelines$current_round()
rankings <- af_pipelines$rankings(session_id, order = "rank", order_direction = "ASC")
players <- af_pipelines$players()
squads <- af_pipelines$squads()

player_stats <- fetch_player_stats_afl(season = 2025)
results_stats <- fetch_results_afl(season = 2025)

af_players_by_round <- af_pipelines$players_by_round()

saveRDS(rankings, here("data","exports","MT_plane_downloads", paste0("rankings.rds")))
saveRDS(player_stats, here("data","exports","MT_plane_downloads", paste0("player_stats.rds")))
saveRDS(results_stats, here("data","exports","MT_plane_downloads", paste0("results_stats.rds")))
saveRDS(af_players_by_round, here("data","exports","MT_plane_downloads", paste0("af_players_by_round.rds")))
saveRDS(players, here("data","exports","MT_plane_downloads", paste0("players.rds")))
saveRDS(squads, here("data","exports","MT_plane_downloads", paste0("squads.rds")))
saveRDS(rounds_2023, here("data","exports","MT_plane_downloads", paste0("rounds_2023.rds")))
saveRDS(players_2023, here("data","exports","MT_plane_downloads", paste0("players_2023.rds")))
saveRDS(players_stats, here("data","exports","MT_plane_downloads", paste0("sc_2025.rds")))
saveRDS(players, here("data","exports","MT_plane_downloads", paste0("sc_2024_diff.rds")))


players <- readRDS(here("data","exports","MT_plane_downloads", paste0("players.rds")))
squads <- readRDS(here("data","exports","MT_plane_downloads", paste0("squads.rds")))

# read in data








# price change r0


sc_data


af_data <- af_data |>
  select(player_id, price, round) |>
  mutate(player_id = paste0("CD_I",player_id))


af_data_6 <- af_data |>
  filter(round == 6)
af_data_1 <- af_data |>
  filter(round == 1)


data_af <- af_data_6 |>
  left_join(af_data_1,
            by = "player_id")


data_af <- data_af |>
  mutate(SeasonPriceChange = price.x - price.y) |>
  select(player_id, SeasonPriceChange)


output <- data_af |>
  left_join(sc_data,
            by = "player_id")


out <- players |>
  mutate(Player = paste0(first_name, " ", last_name)) |>
  select(Player, player_id) |>
  mutate(player_id = paste0("CD_I",player_id)) |>
  left_join(output,
            by = "player_id")

fwrite(out, here("data","exports","2025","_for_mm","zz_adhoc",paste0("season_chg.csv")))













player_stats_afl_2012_to_2024 <- 2012:2024 |> map(fetch_player_stats_afl) |> list_rbind()
saveRDS(player_stats_afl_2012_to_2024, here("data","exports","MT_plane_downloads", paste0("player_stats_afl_2012_to_2024.rds")))


player_stats_afl_2012_to_2024 <- readRDS(here("data","exports","MT_plane_downloads", paste0("player_stats_afl_2012_to_2024.rds")))






data <- sc_api$get_players(year = 2025)
data_2024 <- sc_api$get_players(year = 2024)
saveRDS(data, here("data","exports","MT_plane_downloads", paste0("sc_get_players_2025.rds")))
saveRDS(data_2024, here("data","exports","MT_plane_downloads", paste0("sc_get_players_2024.rds")))


data <- sc_api$get_players(year = 2025)
data_2024 <- sc_api$get_players(year = 2024)
saveRDS(data, here("data","exports","MT_plane_downloads", paste0("sc_get_players_2025.rds")))
saveRDS(data_2024, here("data","exports","MT_plane_downloads", paste0("sc_get_players_2024.rds")))


data_2024 |>
  sc_tabulate$players() |>
  rename(player_id = id)


data_2024 |>
  player_stats()










## MT Code

season <- 2023

players <- readRDS(here("data","exports","MT_plane_downloads", paste0("players_",season,".rds")))
rounds <- readRDS(here("data","exports","MT_plane_downloads", paste0("rounds_",season,".rds")))


num_rounds <- length(rounds)
num_player <- length(players)

rounds_data <- data.frame()

game_by_game <- function(round, match){

  # round <- 1
  # match <- 1

  result <- tryCatch({
    rounds[[round]]$matches[[match]]
  }, error = function(e){
    e
  })

  if(inherits(result,"error")){

    return()

  } else {

    data_round <- data.frame(round = round,
                             team = rounds[[round]]$matches[[match]]$home_squad_id,
                             oppo = rounds[[round]]$matches[[match]]$away_squad_id,
                             season = season)

    data_round_flipped <- data.frame(round = round,
                                     team = rounds[[round]]$matches[[match]]$away_squad_id,
                                     oppo = rounds[[round]]$matches[[match]]$home_squad_id,
                                     season = season)

    export <- bind_rows(data_round, data_round_flipped)

  }

  return(export)


}

for(i in 1:num_rounds){

  for(j in 1:9){

    output <- game_by_game(i,j)
    rounds_data <- bind_rows(rounds_data, output)

  }

}




get_price <- function(player_index, round){

  temp <- players[[player_index]]

  round_num <- names(unlist(temp$stats$prices))
  price <- as.numeric(unlist(temp$stats$prices))

  dt <- data.frame(round_num = round_num,
                   price = price)

  # round <- 1

  price <- dt |>
    filter(round_num == round) |>
    pull(price)


  if(length(price) == 0){

    price <- NA

  }
  return(price)

}


get_score <- function(player_index, round){

  temp <- players[[player_index]]

  round_num <- names(unlist(temp$stats$scores))
  score <- as.numeric(unlist(temp$stats$scores))

  dt <- data.frame(round_num = round_num,
                   score = score)

  score <- dt |>
    filter(round_num == round) |>
    pull(score)


  if(length(score) == 0){

    score <- NA

  }
  return(score)

}


bulid_player_data <- function(player_index){

  temp <- players[[player_index]]

  id <- temp$id
  first_name <- temp$first_name
  last_name <- temp$last_name
  squad_id <- temp$squad_id


  df <- data.frame()

  for(i in c(1:num_rounds)){

    data_round <- data.frame(player_id = id,
                             first_name = first_name,
                             last_name = last_name,
                             team = squad_id,
                             cost = get_price(player_index, i),
                             score = get_score(player_index, i),
                             round = i,
                             season = season)
    df <- bind_rows(df, data_round)

  }

  return(df)

}

final <- data.frame()

for(i in 1:num_player){

  temp_2 <- bulid_player_data(i)
  final <- bind_rows(final, temp_2)

}


final |>
  left_join(rounds_data,
            by = c("season", "round", "team")) |>
  filter(!(is.na(score)))
