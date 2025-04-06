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
  arrow[write_parquet, read_parquet],
  httr2[request, req_perform, resp_body_json, req_headers, req_url_query]
)

# season <- 2023

data_collection <- function(season){

  rounds <- request(paste0("https://fantasy.afl.com.au/data/afl/archive/",season,"/rounds.json")) |>
    req_perform() |>
    resp_body_json()
  players <- request(paste0("https://fantasy.afl.com.au/data/afl/archive/",season,"/players.json")) |>
    req_perform() |>
    resp_body_json()

  num_rounds <- length(rounds)
  num_player <- length(players)

  game_by_game_all <- function(rounds, season) {

    round_match_grid <- expand.grid(round = 1:length(rounds), match = 1:9)


    result_list <- mapply(function(round, match) {
      result <- tryCatch({
        rounds[[round]]$matches[[match]]
      }, error = function(e) {
        return(NULL)
      })

      if(is.null(result)) {
        return(NULL)
      } else {
        data_round <- data.frame(round = round,
                                 team = result$home_squad_id,
                                 oppo = result$away_squad_id,
                                 team_score = result$home_score,
                                 oppo_score = result$away_score,
                                 venue = result$venue_id,
                                 season = season)
        data_round_flipped <- data.frame(round = round,
                                         team = result$away_squad_id,
                                         oppo = result$home_squad_id,
                                         team_score = result$away_score,
                                         oppo_score = result$home_score,
                                         venue = result$venue_id,
                                         season = season)
        return(rbind(data_round, data_round_flipped))
      }
    }, round_match_grid$round, round_match_grid$match, SIMPLIFY = FALSE)


    do.call(rbind, result_list[!sapply(result_list, is.null)])
  }


  rounds_data <- game_by_game_all(rounds, season)


  process_player_data <- function(players, num_rounds, season) {

    extract_stat <- function(player, stat_type) {
      stats <- player$stats[[stat_type]]
      if(length(stats) == 0) return(NULL)

      round_nums <- names(unlist(stats))
      values <- as.numeric(unlist(stats))
      result <- data.frame(round_num = as.integer(round_nums), value = values)
      return(result)
    }


    player_data <- lapply(1:length(players), function(player_index) {
      player <- players[[player_index]]


      prices_df <- extract_stat(player, "prices")
      if(!is.null(prices_df)) {
        names(prices_df)[2] <- "cost"
      }

      scores_df <- extract_stat(player, "scores")
      if(!is.null(scores_df)) {
        names(scores_df)[2] <- "score"
      }


      all_rounds <- data.frame(round = 1:num_rounds)


      player_rounds <- all_rounds


      if(!is.null(prices_df)) {
        player_rounds <- player_rounds |>
          left_join(prices_df, by = c("round" = "round_num"))
      } else {
        player_rounds$cost <- NA
      }

      if(!is.null(scores_df)) {
        player_rounds <- player_rounds |>
          left_join(scores_df, by = c("round" = "round_num"))
      } else {
        player_rounds$score <- NA
      }


      player_rounds$player_id <- player$id
      player_rounds$first_name <- player$first_name
      player_rounds$last_name <- player$last_name
      player_rounds$team <- player$squad_id
      player_rounds$season <- season
      player_rounds$position_1 <- player$positions[[1]]
      player_rounds$position_2 <- tryCatch({
        player$positions[[2]]

      }, error = function(e) {
        NA
      })

      return(player_rounds)
    })


    do.call(rbind, player_data)
  }


  final <- process_player_data(players, num_rounds, season)


  result <- final |>
    left_join(rounds_data, by = c("season", "round", "team")) |>
    mutate(result = if_else(team_score > oppo_score, "W",
                            if_else(team_score == oppo_score, "D", "L"))) |>
    mutate(position_1 = if_else(position_1 == 1, "DEF",
                                if_else(position_1 == 2, "MID",
                                        if_else(position_1 == 3, "RUC","FWD")))) |>
    mutate(position_2 = if_else(is.na(position_2), NA,
                                if_else(position_2 == 1, "DEF",
                                        if_else(position_2 == 2, "MID",
                                                if_else(position_2 == 3, "RUC","FWD"))))) |>
    mutate(position = if_else(is.na(position_2), position_1, paste0(position_1,"/",position_2))) |>
    select(-c(position_1, position_2)) |>
    mutate(af_ownership = 0) |>
    mutate(af_ownership_top_1000 = 0) |>
    mutate(af_ownership_top_100 = 0) |>
    mutate(af_ownership_top_10 = 0) |>
    mutate(af_priced_at = 0) |>
    mutate(af_breakeven = 0)


  return(result)


}


vector <- c(2018:2024)
data <- vector |>
  map(data_collection) |>
  list_rbind()






