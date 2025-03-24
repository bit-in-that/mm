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
  jsonlite[write_json]
)

current_round <- af_pipelines$current_round()

# adhoc adjustment to include the changes in ownershuip
# read in data

data <- data.table::fread(here("data/exports/2025/_for_mm", paste0("sc_ownership_r", current_round,".csv")))
data_previous <- data.table::fread(here("data/exports/2025/_for_mm", paste0("sc_ownership_r", current_round-1,".csv")))

data <- merge(data, data_previous, by = c("Player", "Team"))
data[, OwnershipTop1000_chg := OwnershipTop1000.x - OwnershipTop1000.y]
data[, OwnershipTop100_chg := OwnershipTop100.x - OwnershipTop100.y]
data[, OwnershipTop10_chg := OwnershipTop10.x - OwnershipTop10.y]

data[, OwnershipTotal.y := NULL]
data[, OwnershipTop1000.y := NULL]
data[, OwnershipTop100.y := NULL]
data[, OwnershipTop10.y := NULL]

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here("data/exports/2025/_for_mm", paste0("sc_ownership_r", current_round,"_final.csv")))
write_json(data, path = here("data/exports/2025/_for_mm", paste0("sc_ownership_r", current_round,"_final.json")))

data <- data.table::fread(here("data/exports/2025/_for_mm", paste0("af_ownership_r", current_round,".csv")))
data_previous <- data.table::fread(here("data/exports/2025/_for_mm", paste0("af_ownership_r", current_round-1,".csv")))

data <- merge(data, data_previous, by = c("Player", "Team"))
data[, OwnershipTop1000_chg := OwnershipTop1000.x - OwnershipTop1000.y]
data[, OwnershipTop100_chg := OwnershipTop100.x - OwnershipTop100.y]
data[, OwnershipTop10_chg := OwnershipTop10.x - OwnershipTop10.y]

data[, OwnershipTotal.y := NULL]
data[, OwnershipTop1000.y := NULL]
data[, OwnershipTop100.y := NULL]
data[, OwnershipTop10.y := NULL]

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here("data/exports/2025/_for_mm", paste0("af_ownership_r", current_round,"_final.csv")))
write_json(data, path = here("data/exports/2025/_for_mm", paste0("af_ownership_r", current_round,"_final.json")))


data <- data.table::fread(here("data/exports/2025/_for_mm", paste0("cba_r", current_round,".csv")))
data_previous <- data.table::fread(here("data/exports/2025/_for_mm", paste0("cba_r", current_round-1,".csv")))

data_previous <- data_previous[, c("playerId", "CBA_PERC", "KI_PERC")]

data <- merge(data, data_previous, by = "playerId")
data[, CBA_PERC_chg := CBA_PERC.x - CBA_PERC.y]
data[, KI_PERC_chg := KI_PERC.x - KI_PERC.y]

data[, CBA_PERC.y := NULL]
data[, KI_PERC.y := NULL]

setnames(data, names(data), gsub(".x","",names(data)))
fwrite(data, here("data/exports/2025/_for_mm", paste0("cba_r", current_round,"_final.csv")))
write_json(data, path = here("data/exports/2025/_for_mm", paste0("cba_r", current_round,"_final.json")))
