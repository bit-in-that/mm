
# source the libraries
source(here("a0_inputs","00_parameters_libraries.R"))

# read in data
data <- fromJSON(here("a0_inputs","player-stats-af.json"), flatten = TRUE)

# remove non-ownership columns
export <- data %>%
            select(Player, Team, OwnershipTotal, OwnershipTop1000, OwnershipTop100, OwnershipTop10)

# export the data
write_csv(export, here("b1_data_exports","ownership_template.csv"))
