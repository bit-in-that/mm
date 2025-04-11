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

  # Fetch rounds and players JSON
  rounds <- request(paste0("https://fantasy.afl.com.au/data/afl/archive/", season, "/rounds.json")) |>
    req_perform() |>
    resp_body_json()

  players <- request(paste0("https://fantasy.afl.com.au/data/afl/archive/", season, "/players.json")) |>
    req_perform() |>
    resp_body_json()

  num_rounds <- length(rounds)

  # Extract round-by-round match data
  game_by_game_all <- function(rounds, season) {
    round_match_grid <- expand.grid(round = 1:length(rounds), match = 1:9)

    result_list <- mapply(function(round, match) {
      result <- tryCatch(rounds[[round]]$matches[[match]], error = function(e) NULL)

      if (is.null(result)) return(NULL)

      data_home <- data.frame(round = round,
                              team = result$home_squad_id,
                              oppo = result$away_squad_id,
                              team_score = result$home_score,
                              oppo_score = result$away_score,
                              venue = result$venue_id,
                              season = season)

      data_away <- data.frame(round = round,
                              team = result$away_squad_id,
                              oppo = result$home_squad_id,
                              team_score = result$away_score,
                              oppo_score = result$home_score,
                              venue = result$venue_id,
                              season = season)

      rbind(data_home, data_away)
    }, round_match_grid$round, round_match_grid$match, SIMPLIFY = FALSE)

    bind_rows(result_list)
  }

  rounds_data <- game_by_game_all(rounds, season)

  # Process player stats
  process_player_data <- function(players, num_rounds, season) {

    extract_stat <- function(player, stat_type) {
      tryCatch({
        stats <- player$stats[[stat_type]]
        if (length(stats) == 0) return(NULL)

        round_nums <- names(unlist(stats))
        values <- as.numeric(unlist(stats))
        if (length(round_nums) != length(values)) return(NULL)

        data.frame(round_num = as.integer(round_nums), value = values)
      }, error = function(e) NULL)
    }

    player_data <- lapply(players, function(player) {
      prices_df <- extract_stat(player, "prices")
      scores_df <- extract_stat(player, "scores")

      all_rounds <- data.frame(round = 1:num_rounds)

      player_rounds <- all_rounds |>
        mutate(
          cost = if (!is.null(prices_df)) prices_df$value[match(round, prices_df$round_num)] else NA_real_,
          score = if (!is.null(scores_df)) scores_df$value[match(round, scores_df$round_num)] else NA_real_,
          player_id = player$id,
          first_name = player$first_name,
          last_name = player$last_name,
          team = player$squad_id,
          season = season,
          position_1 = player$positions[[1]],
          position_2 = tryCatch(player$positions[[2]], error = function(e) NA)
        )

      return(player_rounds)
    })

    bind_rows(player_data)
  }

  final <- process_player_data(players, num_rounds, season)

  result <- final |>
    left_join(rounds_data, by = c("season", "round", "team")) |>
    mutate(
      result = case_when(
        team_score > oppo_score ~ "W",
        team_score == oppo_score ~ "D",
        team_score < oppo_score ~ "L",
        TRUE ~ NA_character_
      ))

  return(result)
}

vector <- c(2018:2024)
data <- vector |>
  map(data_collection) |>
  list_rbind()

af_players <- af_pipelines$players()

export <- data |>
  filter(player_id %in% c(af_players$player_id)) |>
  mutate(player_id = paste0("CD_I",player_id)) |>
  mutate(Player = paste0(first_name, " ", last_name)) |>
  select(Season = season,
         Round = round,
         player_id,
         Player,
         squad_id = team,
         oppo,
         AF = score
         )




