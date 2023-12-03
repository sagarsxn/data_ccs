# function combines all dacnet CSV files into one. At the same time, it
# standardizes column names and adds a few new columns such as wave,
# clean crop name, etc.

function combine_all_dacnet_csv_files(dir_temp_files, out_path)

    # get all file paths
    csv_files = filter(readdir(dir_temp_files, join=true)) do f
        endswith(f, ".csv") && startswith(basename(f), "P")
    end

    # initialize master dataframe
    df_master = DataFrame()

    # loop over all files
    n_files = length(csv_files)
    for (i, f) in enumerate(csv_files)

        # start and end year
        start_year = parse(Int, match(r"P(\d{4})", basename(f)).captures[1])
        end_year = start_year + 1

        # read file
        df = CSV.read(f, DataFrame)

        # sometimes, column names start in row #2; this part of the code
        # catches that 
        if ~("state" in lowercase.(names(df)))
            println(f)
            df = CSV.read(f, DataFrame; header=2)
        end

        # clean column names
        rename!(clean_dacnet_colname, df)

        # create start and end years
        df.start_year .= start_year
        df.end_year .= end_year

        # check if notes present; easiest way to do is to check whether
        # crop area is missing, and if yes, drop those rows
        if sum(ismissing.(df.crop_area_ha)) > 0
            println("\n=== Notes in $f")
            dropmissing!(df, :crop_area_ha)
        end

        # append to master data
        append!(df_master, df, cols=:union)

        # print progress
        @info "Finished $(basename(f)) | Remaining: $(n_files - i)"
    end

    # clean crop names
    df_master.clean_crop = clean_crop_name.(df_master.crop)

    # clean state names
    df_master.state = clean_state_name.(df_master.state)

    # clean year variable
    df_master.year = df_master.start_year

    # get wave
    df_master.wave = get_wave.(df_master.year)

    # get parcel, plot, and season
    df_master = get_pps(df_master)

    # get cultivator code
    df_master = get_cultivator_code(df_master)

    # get farmer id
    df_master = get_farmer_id(df_master)

    # create row totals
    df_master = get_var_totals(df_master)

    # re-arrange the order of variables
    id_vars =
        string.([
            :wave,
            :year,
            :state,
            :zone_code,
            :tehsil_code,
            :size_group,
            :cultivator_code,
            :farmer_id,
            :parcel_plot_season_pps,
            :parcel,
            :plot,
            :season,
            :crop,
            :clean_crop,
            :crop_area_ha,
        ])
    q_vars = names(df_master, r"qty_")
    v_vars = names(df_master, r"val_")
    main_vars = [id_vars; q_vars; v_vars]
    other_vars = [c for c in names(df_master) if ~(c in main_vars)]
    vars = [main_vars; other_vars]
    df_master = df_master[:, vars]

    # save master dataset
    CSV.write(out_path, df_master)

    return df_master
end

## ------------------------------------------------------------------------
##
## HELPER FUNCTIONS
##
## ------------------------------------------------------------------------

# function returns a full list of all column names in all dacnet files

function get_all_raw_column_names_in_dacnet(dir_temp_files)

    # initialize column names
    col_names = []

    # get all file paths
    csv_files = filter(readdir(dir_temp_files, join=true)) do f
        endswith(f, ".csv") && startswith(basename(f), "P")
    end

    # read all files
    @showprogress for f in csv_files
        df = CSV.read(f, DataFrame)

        # sometimes, column names start in row #2; this part of the code
        # catches that 
        if ~("state" in lowercase.(names(df)))
            println(f)
            df = CSV.read(f, DataFrame; header=2)
        end

        # loop over all column names and add to master list, if not
        # already added
        for c in names(df)
            # c = clean_colname(c)
            if ~(c in col_names)
                push!(col_names, c)
            end
        end
    end

    # save column names in a .csv
    colnames = sort(col_names)
    df = DataFrame(column_name=col_names)
    CSV.write(joinpath(dir_data, "temp", "dacnet_raw_column_names.csv"), df)

    return colnames
end

# this function helps clean column names; it takes in a suffix e.g.
# "_kg" and tags those variables using a given prefix e.g. "qty"

function replace_suffix(x, suffix, prefix)
    return ifelse(endswith(x, suffix), prefix * replace(x, suffix => ""), x)
end

# this function helps clean column names; it takes in a keyword e.g.
# "labour" and turns variables such as "qty_family_labour" into
# "qty_lab_family" where the new keyword "lab" is provided

function rename_using_keywords(x, old, new)
    re = Regex("_(\\w+)_($old)")
    m = match(re, x)
    if isnothing(m)
        return x
    else
        suffix = m.captures[1]
        return replace(x, m.match => "_$(new)_$(suffix)")
    end
end

# function takes in a column name and standardizes it
# across all years

function clean_dacnet_colname(s)
    x = replace(strip(lowercase(s)), "." => "", "%age" => "pct")
    x = replace(join(split(x), "_"), "age_group/code" => "age_group")
    x = replace(x, "%" => "", "(" => "", ")" => "", "/" => "_")
    x = replace(x, "owned" => "own")

    if x == "area_of_all_selected_crops_in_village_ha"
        x = "area_of_selected_crops_in_village"
    end

    chk = [
        "area_of_all_selected_crops_in_zone_ha",
        # "area_of_crop_in_zone_ha",
        "area_of_selected_crop_in_zone_ha",
    ]
    if in(x, chk)
        x = "area_of_selected_crops_in_zone"
    end

    chk = [
        "sample_no_of_growers_in_size_group",
        "sample_size_group-_no_of_growers",
    ]
    if in(x, chk)
        x = "sample_size_group_no_of_growers"
    end

    x = replace(x, "tehsil_no" => "tehsil_code", "zone_no" => "zone_code")
    x = replace(
        x,
        "maximum_rent_pct_of_gvo" => "maximum_rent_pct_of_production",
        "minimum_rent_pct_of_gvo" => "minimum_rent_pct_of_production",
    )

    # ---------- qty variables ----------
    prefix = "qty_"
    x = replace_suffix(x, "_qtls", prefix)
    x = replace_suffix(x, "_qtl", prefix)
    x = replace_suffix(x, "_hrs", prefix)
    x = replace_suffix(x, "_qty_kg", prefix)
    x = replace_suffix(x, "_kg", prefix)

    # ---------- value variables ----------
    prefix = "val_"
    x = replace_suffix(x, "_value_rs", prefix)
    x = replace_suffix(x, "_rs", prefix)

    # rename key variables
    x = replace(x, "fertiliser" => "fert")
    x = rename_using_keywords(x, "fert", "fert")
    x = rename_using_keywords(x, "product", "product")
    x = rename_using_keywords(x, "animal_labour", "animal")
    x = rename_using_keywords(x, "labour", "labor")
    x = rename_using_keywords(x, "irrigation_machine", "irrigation")
    x = rename_using_keywords(x, "machine", "machine")

    return x
end

# function assigns a "wave" to each year:
# Wave 1: 1999-00, 2000-01, 2001-02
# Wave 2: 2002-03, 2003-04, 2004-05
# Wave 3: 2005-06, 2006-07, 2007-08
# Wave 4: 2008-09, 2009-10, 2010-11
# Wave 5: 2011-12, 2012-13, 2013-14
# Wave 6: 2014-15, 2015-16, 2016-17
# Wave 7: 2017-18, 2018-19, 2019-20

function get_wave(file_start_year)
    if file_start_year in [1999, 2000, 2001]
        return 1
    elseif file_start_year in [2002, 2003, 2004]
        return 2
    elseif file_start_year in [2005, 2006, 2007]
        return 3
    elseif file_start_year in [2008, 2009, 2010]
        return 4
    elseif file_start_year in [2011, 2012, 2013]
        return 5
    elseif file_start_year in [2014, 2015, 2016]
        return 6
    elseif file_start_year in [2017, 2018, 2019]
        return 7
    else
        error("Invalid year: $file_start_year")
    end
end

# function builds parcel, plot, and season from pps

function get_pps(df_raw)
    df = deepcopy(df_raw)
    col = :parcel_plot_season_pps
    @rtransform!(df, :temp = string(Int($col)))
    @rtransform!(
        df,
        :parcel = parse(Int, :temp[1]),
        :plot = parse(Int, :temp[2:(end-1)]),
        :season = parse(Int, :temp[end])
    )
    return df[:, Not(:temp)]
end

# each village has multiple "cultivators" or farmers; sometimes they are
# reported in a variable called "tehsil_cultivator", and other times
# they are reported separately in "cultivator_no"; this function
# combines them into a single column
function get_cultivator_code(df_raw)
    df = deepcopy(df_raw)
    @rtransform!(df, @passmissing :temp = string(Int(:tehsil_cultivator)))

    # if cultivator no is missing, grab the last digit in
    # "tehsil_cultivator"; else, use cultivator no
    col = :cultivator_no
    @rtransform!(
        df,
        :cultivator_code = ~ismissing($col) ? string(Int($col)) : :temp[end]
    )
    return df[:, Not(:temp)]
end

# create an id for each unique farmer
function get_farmer_id(df_raw)
    df = deepcopy(df_raw)

    # get unique list of all farmers
    grp = [:wave, :state, :zone_code, :tehsil_code, :cultivator_code]
    df_farmers = sort(unique(df[:, grp]))
    @transform!(df_farmers, :farmer_id = 1:nrow(df_farmers))

    # merge back onto the main data
    df = outerjoin(df, df_farmers, on=grp, source=:merge)
    return df[:, Not(:merge)]
end

# function standardizes state names 

function clean_state_name(s)
    x = strip(s)
    x = replace(
        x,
        "Chattisgarh" => "Chhattisgarh",
        "Jharkand" => "Jharkhand",
        "Orissa" => "Odisha",
    )
    return x
end

# function standardizes crop names
# NOTE: lentil and masur need to be combined into one at some point

function clean_crop_name(c)
    x = lowercase(c)
    if occursin("arhar", x)
        return "arhar"
    elseif occursin("coconut", x)
        return "coconut"
    elseif occursin("cotton", x)
        return "cotton"
    elseif startswith(x, "gram") # since "gram" appears in other crops too
        return "gram"
    elseif (occursin("masur", x) | occursin("masoor", x))
        return "masur"
    elseif occursin("moong", x)
        return "moong"
    elseif occursin("mustard", x)
        return "mustard_rapeseed"
    elseif occursin("nigerseed", x)
        return "nigerseed"
    elseif occursin("paddy", x)
        return "paddy" # ! 2011, 2012, and 2013 exclude Basmati for some reason
    elseif occursin("pea", x)
        return "pea"
    elseif occursin("sesamum", x)
        return "sesamum"
    elseif occursin("urad", x)
        return "urad"
    else
        return replace(strip(x), "," => "_")
    end
end

# function computes total labor, total machine, and total irrigation
# machine variables 

function get_var_totals(df_raw)
    df = deepcopy(df_raw)

    # labor
    df = get_rowsum(df, :qty_labor_total, names(df, r"^qty_labor"))
    df = get_rowsum(df, :val_labor_total, names(df, r"^val_labor"))

    # animal
    df = get_rowsum(df, :qty_animal_total, names(df, r"^qty_animal"))
    df = get_rowsum(df, :val_animal_total, names(df, r"^val_animal"))

    # machine
    df = get_rowsum(df, :qty_machine_total, names(df, r"^qty_machine"))
    df = get_rowsum(df, :val_machine_total, names(df, r"^val_machine"))

    # irrigation
    df = get_rowsum(df, :qty_irrigation_total, names(df, r"^qty_irrigation"))
    df = get_rowsum(df, :val_irrigation_total, names(df, r"^val_irrigation"))

    # value of output
    df = get_rowsum(df, :val_prod_total, names(df, r"^val_prod"))

    return df
end

# function takes in a dataframe, a list of columns, and the name for a
# new column that equals the rowsum of the list of columns; it also
# skips missing values in any of the columns
function get_rowsum(df_raw, new_var, columns)
    return transform(
        df_raw,
        AsTable(columns) => ByRow(sum ∘ skipmissing) => new_var,
    )
end