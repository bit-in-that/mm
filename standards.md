# Standards

This documents lays out the standards and conventions for this repository. It is designed to be readable by both humans and LLMs.

LLMs should ask the user before proceeding if they are thinking it is necessary to write code that breaks these conventions.

# Developer setup

Those working on this repository are using Windows computers and RStudio as their IDE.


# R coding standards

## Package preferences

Please use the following packages/frameworks for the following purposes:

- The `here` package should be used for getting paths

- tidyverse should be used for data manipulation, including :
    
    - `dplyr` and `tidyr` for data frame operations, 

    - `purrr` for functional programming (e.g. the `map_` family of functions)

    - `stringr` for regular expressions and other string operations

    - Consequently, `data.table` should only be used for reading and writing csvs (never data manipulations)

- Use `httr2` for hitting apis

- `jsonlite` should be used for reading and writing json files

- `arrow` should be used for reading and writing parqet files

- `modules` package should be used for referencing functions from other files or packages 

## Coding practices

- A project-based workflow should be used:

   -  RStudio projects (.RProj files) should be used to open RStudio in order to set the working directory

   -  File paths should be set using the `here()` function from the `here` package

   -  All file paths should be relative

# Data standards

Production data is to be stored in a sqlite database in `data/db` folder in this repository. 

Some outputs will also be stored in other formats:

- If they need to be in a compressed format that retains column types, use `.parquet` files

- If they need to be in a format that is easy for humans to open and read, use `.csv` files

- If more niche R objects that are not in a neat rectangular table format need to be saved, use `.Rds` files

# Naming conventions

- All names (file names, variable names, column names) should be in camel case

- Function names should start with a verb and end with a vowel (e.g. `get_data()`)

- Abbreviations in names should be avoided with the following exceptions:

    - afl: Australian Football League
    - af: AFL Fantasy
    - sc: supercoach

- Names should be informative and consistent (e.g. if there is are two functions that do the same thing for both AFL Fantasy and Supercoach, the only difference in their name should be a `af` or `sc`)

# Repository structure

## Folders

The repository contains the following folders

- `R`: contrain R code, inputs and RStudio project

    - `00_inputs`: where parameters and other inputs are kept

    - `01_modules`: functions used by other scripts

    - `02_data_pipelines`: scripts for retreiving data via apis, manipulating it into tables and saving it to a database

    - `03_data_pipelines`: scripts for retreiving and cleaning data

    - `99_adhoc`: one-off or wuick code
    

- `data`: output all data here

    - `db`: where the sqlite databases are stored

        - `migrations`: `.sql` files that contain `CREATE TABLE` statements that specify the schema and relationships of all tables and accompanying `.cmd` files that execute them 

    - `exports`: where data exported data extracts are saved in `.csv`, `.parquet` and `.Rds` formats
 
    - `temp`: where temporary data is stored (`.gitignore`'d)

## Files

- `README.md`: explains this repository

- `standards.md`: this document, outlining the standards and conventions for the benefit of humans and LLMs alike

- `R/R.Rproj`: Rstudio project (should always be used to open RStudio as to set the correct working directory)
