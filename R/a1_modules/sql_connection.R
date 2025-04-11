
box::use(
  dotenv[load_dot_env],
  RMySQL[MySQL],
  DBI[dbConnect]
)

load_dot_env()

connect_to_mysql <- function() {

  db_host <- "ls-7189a7c3f9e8e50019ede4ba0e86c98674eaf21a.czyw0iuiknog.ap-southeast-2.rds.amazonaws.com"
  db_name <- "mm_data"
  db_user <- "dbmasteruser"
  db_password <- Sys.getenv("sql_password")
  db_port <- 3306  # Default MySQL port

  con <- dbConnect(
    RMySQL::MySQL(),
    host = db_host,
    dbname = db_name,
    user = db_user,
    password = db_password,
    port = db_port
  )

  return(con)
}
