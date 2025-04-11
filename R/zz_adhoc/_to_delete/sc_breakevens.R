

box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  rvest[read_html, html_nodes, html_table],
  purrr[if_else],
  readxl[read_excel]
)


#' @export
sc_breakevens <- function(){

  url <- "https://www.footywire.com/afl/footy/supercoach_breakevens"
  adhoc_changes_footywire <- read_excel(here("data","inputs","adhoc_changes.xlsx"), sheet = "footywire")

  # Read the HTML content
  page <- read_html(url)

  # Extract the table data
  player_table <- page |>
    html_nodes("table") |>
    html_table()

  player_table <- player_table[[1]]

  raw_data <- player_table |>
    select("X1", "X6") |>
    mutate(breakeven = as.numeric(X6)) |>
    filter(!is.na(breakeven)) |>
    select(-X6)

  raw_data |>
    filter(breakeven == 64)



  clean_data <- raw_data |>
    mutate(player_info = if_else(X1 == "Bailey WilliamsB. Williams \nRUC", "Bailey J WilliamsB. Williams \nRUC", X1)) |>
    mutate(
      dot_pos = str_locate(player_info, "\\.")[, 1],
      player_name = ifelse(!is.na(dot_pos), str_sub(player_info, 1, dot_pos - 2), NA_character_)
    ) |>
    select(player_name, breakeven)

  clean_data <- clean_data |>
    mutate(player_name = if_else(player_name == "Bailey J Williams", "Bailey J. Williams", player_name))

  clean_data |>
    left_join(adhoc_changes_footywire,
              by = c("player_name" = "mm_name")) |>
    mutate(player_name = if_else(is.na(af_name),player_name,af_name)) |>
    select(-af_name)

  return(clean_data)

}





