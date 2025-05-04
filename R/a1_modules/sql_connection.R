
box::use(
  dotenv[load_dot_env],
  RMySQL[MySQL],
  DBI[dbConnect, dbWriteTable, dbExecute, dbGetQuery]
)

load_dot_env()

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


data <- read_csv(here("data","exports","2025","_for_mm",paste0("b_round_0",current_round),paste0("mm_master_table_r_",current_round,".csv")))



dbWriteTable(con, name = "master_table_test", value = data, row.names = FALSE)
input <- dbGetQuery(con, "SELECT * FROM master_table_test")

