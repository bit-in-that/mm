box::use(
  httr2[request, req_perform, resp_body_json]
)

get_players <- function() {

  req <- request("https://fantasy.afl.com.au/data/afl/coach/players.json")

  resp <- req |>
    req_perform()

  resp |>
    resp_body_json()

}
