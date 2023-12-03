# India: Plot-Level Cropping Data Collected Under the Cost of Cultivation Scheme

This repository contains the code and raw data files used to construct the dataset used in Garg and Saxena (2023). The dataset includes detailed, plot-level information on crop and input decisions made by a sample of Indian farmers. The data are collected by the Directorate of Economics and Statistics, Ministry of Agriculture, Government of India and can be downloaded from the [website](https://eands.dacnet.nic.in/Plot-Level-Summary-Data.htm) of the Directorate of Economics and Statistics, Ministry of Agriculture, Government of India.

We download the publicly-available data for the years 2000-01 to 2019-20. These data are spread across hundreds of worksheets, which we clean and combine. To construct the dataset:
1. Download all files under `data/raw`
2. Follow the steps in the script `code/_main.jl` to clean and combine the data

The output of the script is a single CSV file, `data/clean/dacnet_raw_all_years.csv`, which contains the cleaned and combined data. For estimating the crop-choice model in Garg and Saxena (2023), we restrict the data to relevant crops and years; however, the above script does not do this. If you have any questions, please email me at [sagarsxn@penn.edu](mailto:sagarsxn@penn.edu). 