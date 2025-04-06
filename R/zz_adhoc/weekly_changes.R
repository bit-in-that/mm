box::use(
  ../a1_modules/sc_pipelines,
  ../a1_modules/af_pipelines
)
box::use(
  dotenv[load_dot_env],
  here[here],
  dplyr[...],
  purrr[...],
  data.table[fwrite, setnames],
  jsonlite[write_json],
  data.table[...],
  tidyr[replace_na]
)

current_round <- af_pipelines$current_round()

# adhoc adjustment to include the changes in ownershuip
# read in data

map <- data.table::fread("C:/Users/michael.thomas/Desktop/Michael Thomas/Moreira's Magic/mm/data/received/mm_naming_map.csv")

# results_avg_win <- read_csv(here("data","exports","2025","_for_mm","last_3",paste0("results_avg_win",current_round,".csv")))

data <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round),paste0("sc_ownership_r_",current_round,".csv")))
data_previous <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round-1),paste0("sc_ownership_r",current_round-1,".csv")))

data <- merge(data, data_previous, by = c("Player", "Team"))
data[, OwnershipTop1000_chg := OwnershipTop1000.x - OwnershipTop1000.y]
data[, OwnershipTop100_chg := OwnershipTop100.x - OwnershipTop100.y]
data[, OwnershipTop10_chg := OwnershipTop10.x - OwnershipTop10.y]

data[, OwnershipTotal.y := NULL]
data[, OwnershipTop1000.y := NULL]
data[, OwnershipTop100.y := NULL]
data[, OwnershipTop10.y := NULL]

data <- merge(data, map, by.x = "Player", by.y = "af_name", all.x = T)
data[, Player := ifelse(is.na(mm_name),Player,mm_name)]
data[, mm_name := NULL]


### Supercoach

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round),paste0("sc_ownership_r_",current_round,"_final_mm.csv"))))
# write_json(data, here(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round),paste0("sc_ownership_r_",current_round,"_final.json"))))














data <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round),paste0("af_ownership_r_",current_round,".csv")))
data_previous <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round-1),paste0("af_ownership_r",current_round-1,".csv")))


data <- merge(data, data_previous, by = c("Player", "Team"))
data[, OwnershipTop1000Chg := OwnershipTop1000.x - OwnershipTop1000.y]
data[, OwnershipTop100Chg := OwnershipTop100.x - OwnershipTop100.y]
data[, OwnershipTop10Chg := OwnershipTop10.x - OwnershipTop10.y]

data[, OwnershipTotal.y := NULL]
data[, OwnershipTop1000.y := NULL]
data[, OwnershipTop100.y := NULL]
data[, OwnershipTop10.y := NULL]

# data <- merge(data, map, by.x = "Player", by.y = "af_name", all.x = T)
# data[, Player := ifelse(is.na(mm_name),Player,mm_name)]
# data[, mm_name := NULL]

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here("data","exports","2025","_for_mm",paste0("b_round_0", current_round), paste0("af_ownership_r_", current_round,"_final.csv")))
















#### CBA's and KI's

data <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round), paste0("cba_r_", current_round,".csv")))
data_previous <- data.table::fread(here("data","exports","2025","_for_mm",paste0("b_round_0", current_round-1), paste0("cba_r", current_round-1,".csv")))


data_previous_no_r2 <- data_previous[!(playerId %in% c(data$playerId)), ]

data_previous_no_r2[, roundNumber := current_round-1]
data_previous_no_r2[, c("TOG", "AF", "CBA", "KI", "CBA_PERC", "KI_PERC", "SC", "TeamCBA", "TeamKI") := 0]

data <- rbind(data, data_previous_no_r2)

data_previous <- data_previous[, c("playerId", "CBA_PERC", "KI_PERC")]

data <- merge(data, data_previous, by = "playerId", all.x = T)

data[, CBA_PERC.y := ifelse(is.na(CBA_PERC.y),0,CBA_PERC.y)]
data[, KI_PERC.y := ifelse(is.na(KI_PERC.y),0,KI_PERC.y)]

data[, CBA_PERC_chg := CBA_PERC.x - CBA_PERC.y]
data[, KI_PERC_chg := KI_PERC.x - KI_PERC.y]

data[, CBA_PERC.y := NULL]
data[, KI_PERC.y := NULL]

data <- as.data.frame(data)

data <- data |>
  mutate(teamName = if_else(teamName == "Adelaide Crows", "Adelaide",
                            if_else(teamName == "Footscray", "Western Bulldogs",
                                    if_else(teamName == "Gold Coast SUNS", "Gold Coast Suns",
                                            if_else(teamName == "GWS GIANTS", "GWS Giants", teamName))))) |>
  mutate(across(everything(), ~replace_na(., 0)))

data <- as.data.table(data)

# data <- merge(data, map, by.x = "name", by.y = "af_name", all.x = T)
# data[, name := ifelse(is.na(mm_name),name,mm_name)]
# data[, mm_name := NULL]

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here("data","exports","2025","_for_mm",paste0("b_round_0", current_round), paste0("cba_r_", current_round,"_final.csv")))
# write_json(data, here("data","exports","2025","_for_mm",paste0("b_round_0", current_round), paste0("cba_r_", current_round,"_final.json")))


