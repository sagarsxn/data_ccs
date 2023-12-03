# This file combines all .xls files and .xlsx worksheets into a single
# .csv file. The raw data can be found at .../data/raw.
# Follow the steps below to run the code:

## ---------- packages ----------
using CSV, DataFrames, DataFramesMeta, Dates, ProgressMeter

## ---------- directories ----------
dir_curr = dirname(@__FILE__)
dir_root = dirname(dir_curr)
dir_data = joinpath(dir_root, "data")

## ---------- STEP 1 ----------

# 1. Convert all .xls files to csv using python
# run the script: dacnet_convert_xl_to_csv.py

## ---------- STEP 2 ----------

# 2. Combine all .csv files into a single .csv file; this script also
#    renames variables consistently
include(joinpath(dir_curr, "dacnet_combine_all_files.jl"))

# where are the .csv files?
dir_temp_files = joinpath(dir_data, "raw", "temp")

# where will the combined .csv file be saved?
path_output = joinpath(dir_data, "clean", "dacnet_raw_all_years.csv")

# combine all .csv files into a single .csv file
combine_all_dacnet_csv_files(dir_temp_files, path_output)

## ----------------------------------------
## TEMP
## ----------------------------------------

# quick look at all possible "raw" column names
colnames = get_all_raw_column_names_in_dacnet(dir_temp_files)