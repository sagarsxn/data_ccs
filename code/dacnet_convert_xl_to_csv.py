# This file converts the raw DACNET excel files to CSV files

# %% ---------- packages ----------
import os
import xlrd
import pandas as pd

# %% ---------- directories ----------
dir_curr = os.path.dirname(os.path.abspath(__file__))
dir_root = os.path.dirname(dir_curr)
dir_data = os.path.join(dir_root, "data")

# %% ---------- todo & notes ----------

# %%
# ------------------------------------------------------------------------
#
# SETUP
#
# ------------------------------------------------------------------------

# %% ---------- directories ----------
dir_raw_files = os.path.join(dir_data, "raw")
dir_csv_files = os.path.join(dir_data, "raw", "temp")

# make directory if it doesn't exist
if not os.path.exists(dir_csv_files):
    os.makedirs(dir_csv_files)


# function to get all relevant files in the directory
def get_list_of_raw_dacnet_files(dir_raw_files):
    # get all raw files in directory that start with "P" and end with ".xls"
    # or ".xlsx"
    all_xl_files = [
        f
        for f in os.listdir(dir_raw_files)
        if f.startswith("P") and (f.endswith(".xls") or f.endswith(".xlsx"))
    ]

    # convert to dataframe
    df_files = pd.DataFrame(all_xl_files, columns=["file_name"])
    df_files = df_files.sort_values(by="file_name")

    # file year
    df_files["file_year"] = df_files["file_name"].str[1:8]
    df_files["file_start_year"] = df_files["file_year"].str[:4].astype(int)

    # file type
    df_files["file_type"] = df_files["file_name"].str[8:]

    # file path
    df_files["full_path"] = [
        os.path.join(dir_raw_files, x) for x in df_files["file_name"]
    ]

    return df_files


# %%
# ----------------------------------------
# NOTES
# ----------------------------------------

# For years 2000-01 to 2010-11, all data are in a single sheet called
# "ccpcyyyy", where yyyy is the starting year

# %%
# ------------------------------------------------------------------------
#
# 2000-01 to 2010-11
#
# ------------------------------------------------------------------------

# relevant files
df_files = get_list_of_raw_dacnet_files(dir_raw_files)
df_files = df_files[df_files["file_start_year"].isin(range(2000, 2011))]

# loop over files and save them as csv
for i in range(df_files.shape[0]):
    # get file path
    path = df_files["full_path"].iloc[i]

    # get file year
    file_year = df_files["file_year"].iloc[i]

    # sheet name
    file_start_year = df_files["file_start_year"].iloc[i]
    sheet_name = f"ccpc{file_start_year}"

    # load the workbook
    xl_wkbook = pd.ExcelFile(path)

    # find the correct sheet name
    sheet_name = ""
    for sheet in xl_wkbook.sheet_names:
        if sheet.startswith("ccpc"):
            sheet_name = sheet
            break

    # read the relevant sheet
    df = xl_wkbook.parse(sheet_name)

    # save as csv
    out_path = os.path.join(dir_csv_files, f"P{file_year}.csv")
    df.to_csv(out_path, index=False)

    # print
    print("Saved: " + f"P{file_year}.csv")

# %%
# ------------------------------------------------------------------------
#
# 2011-12 to 2019-20
#
# ------------------------------------------------------------------------

# relevant files
df_files = get_list_of_raw_dacnet_files(dir_raw_files)
df_files = df_files[df_files["file_start_year"].isin(range(2011, 2020))]


# loop over files and save them as csv
for i in range(df_files.shape[0]):
    # get file path
    path = df_files["full_path"].iloc[i]

    # get file year
    file_year = df_files["file_year"].iloc[i]

    # sheet name
    file_start_year = df_files["file_start_year"].iloc[i]
    sheet_name = f"ccpc{file_start_year}"

    # load the workbook
    xl_wkbook = pd.ExcelFile(path)
    wk_sheets = xl_wkbook.sheet_names

    # if "index" or "sheet" not in sheet name, load the worksheet and convert to CSV
    for sheet in wk_sheets:
        # convert sheet name to lowercase
        s = sheet.lower()

        if "index" not in s and "sheet" not in s:
            # load the worksheet
            df = pd.read_excel(xl_wkbook, sheet_name=sheet)

            # Save as CSV
            out_path = os.path.join(dir_csv_files, f"P{file_year}_{sheet}.csv")
            df.to_csv(out_path, index=False)
            print("Saved: " + f"P{file_year}_{sheet}.csv")


# %%
