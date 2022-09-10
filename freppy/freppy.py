""" freppy.py : python tools for frepp output """

import datetime as dt
import json
import multiprocessing
import os
from collections import OrderedDict

import pandas as pd


def get_nc_files(base_dir):

    """This function runs `os.scandir` to yield a list of netCDF files in the pp dir"""

    assert os.path.exists(base_dir), f"Requested path does not exist: {base_dir}"

    def _generator(base_dir):
        for entry in os.scandir(base_dir):
            if entry.is_file() and entry.name.endswith(".nc"):
                yield entry.path
            elif entry.is_dir():
                yield from get_nc_files(entry.path)

    return list(_generator(base_dir))


def parse_date_string(datestring):
    dates = tuple(datestring.split("-"))

    # case of a single year string
    if len(dates) == 1:

        if datestring == "static":
            t0 = None
            t1 = None

        else:
            assert len(dates[0]) == 4, f"Unrecognized date string: {datestring}"
            t0 = dt.datetime(int(dates[0]), 1, 1)
            t1 = dt.datetime(int(dates[0]), 1, 1) - dt.timedelta(days=1)

    # case of a compound year string
    elif len(dates) == 2:

        # dates must be of equal length
        assert len(dates[0]) == len(
            dates[1]
        ), f"Dates are not equal format: {datestring}"

        # yearly data
        if len(dates[0]) == 4:
            t0 = dt.datetime(int(dates[0]), 1, 1)
            t1 = dt.datetime(int(dates[1]) + 1, 1, 1) - dt.timedelta(days=1)

        # monthly data
        elif len(dates[0]) == 6:
            t0 = dt.datetime(int(dates[0][0:4]), int(dates[0][4:6]), 1)

            month_plus_one = 1 if int(dates[1][4:6]) == 12 else int(dates[1][4:6]) + 1
            t1 = dt.datetime(int(dates[1][0:4]), month_plus_one + 1, 1) - dt.timedelta(
                days=1
            )

        # daily data
        elif len(dates[0]) == 8:
            t0 = dt.datetime(int(dates[0][0:4]), int(dates[0][4:6]), int(dates[0][6:8]))
            t1 = dt.datetime(int(dates[1][0:4]), int(dates[1][4:6]), int(dates[1][6:8]))

        # hourly data
        elif len(dates[0]) == 10:
            t0 = dt.datetime(
                int(dates[0][0:4]),
                int(dates[0][4:6]),
                int(dates[0][6:8]),
                int(dates[0][8:10]),
            )
            t1 = dt.datetime(
                int(dates[1][0:4]),
                int(dates[1][4:6]),
                int(dates[1][6:8]),
                int(dates[1][8:10]),
            )

    else:
        raise ValueError(f"Date string has atypical format: {datestring}")

    t0 = "" if t0 is None else t0.isoformat()
    t1 = "" if t1 is None else t1.isoformat()

    return (t0, t1)


def infer_attributes(file_path, warn=False, **kwargs):

    file_path = file_path.replace("/monthly_", "/monthly/")
    file_path = file_path.replace("/annual_", "/annual/")

    # split up the path
    split_path = file_path.split("/")
    _basename = split_path[-1]

    # The activity could be `CMIP` or other MIP name. For internal use
    # it is set to GFDL
    activity_id = kwargs["activity_id"] if "activity_id" in kwargs.keys() else "GFDL"

    # = kwargs[""] if "" in kwargs.keys() else "NOAA-GFDL"

    # Institution ID Name
    institution_id = (
        kwargs["institution_id"] if "institution_id" in kwargs.keys() else "NOAA-GFDL"
    )

    # Source ID is typically a model name, e.g. `GFDL-CM4`. This is not
    # practical for development, so here we use the full experiment name
    source_id = kwargs["source_id"] if "source_id" in kwargs.keys() else split_path[-8]

    # Experiment ID refers to a formal cmip experiment name, defined in the
    # controlled vocabulary. Examples include `historical` or `piControl`.
    # This is ill-defined in development mode, so we set to an empty string
    experiment_id = kwargs["experiment_id"] if "experiment_id" in kwargs.keys() else ""

    # Ensemble member ID
    member_id = kwargs["member_id"] if "member_id" in kwargs.keys() else "r1i1p1f1"

    # The table id refers to the DRS values, e.g. `Amon`, `Omon`. These are not
    # defined at GFDL, so here we substitute the time freqency, e.g. `monthly`, `annual`
    table_id = kwargs["table_id"] if "table_id" in kwargs.keys() else split_path[-3]

    # The variable name is inferred from the the file path
    variable_id = (
        kwargs["variable_id"]
        if "variable_id" in kwargs.keys()
        else os.path.splitext(_basename)[0].split(".")[-1]
    )

    # Native grid (`gn`) is assumed unless otherwise specified
    grid_label = kwargs["grid_label"] if "grid_label" in kwargs.keys() else "gn"

    # GFDL-specific attributes have the `gfdl_` prefix
    gfdl_freq = split_path[-2]
    gfdl_pptype = split_path[-4]
    gfdl_component = split_path[-5]

    # infer the date range
    _basename_split = _basename.split(".")
    if len(_basename_split) >= 4:
        gfdl_start_time, gfdl_end_time = parse_date_string(_basename.split(".")[1])
    else:
        gfdl_start_time = ""
        gfdl_end_time = ""

    # Fix the path if average file was detected
    if gfdl_pptype == "av":
        file_path = file_path.replace("/monthly/", "/monthly_")
        file_path = file_path.replace("/annual/", "/annual_")

    return (
        activity_id,
        institution_id,
        source_id,
        experiment_id,
        member_id,
        table_id,
        variable_id,
        grid_label,
        gfdl_freq,
        gfdl_pptype,
        gfdl_component,
        gfdl_start_time,
        gfdl_end_time,
        file_path,
    )


def catalog_from_dir(base_dir, filename="catalog"):

    subdirs = [f.path for f in os.scandir(base_dir) if f.is_dir()]
    # subdirs = [x for x in subdirs if "ice" in x]

    # Set up a multiprocessing thread pool
    pool = multiprocessing.pool.ThreadPool()

    # Run the multiprocessing pool
    file_list = pool.map(get_nc_files, subdirs)

    # Flatten the list of files
    file_list = [item for sublist in file_list for item in sublist]

    # Infer attributes from filename
    file_list = [infer_attributes(x) for x in file_list]

    df = pd.DataFrame(
        file_list,
        columns=[
            "activity_id",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "gfdl_freq",
            "gfdl_pptype",
            "gfdl_component",
            "gfdl_start_time",
            "gfdl_end_time",
            "path",
        ],
    )

    df.to_csv(f"{filename}.csv")

    descriptor = OrderedDict()
    descriptor["esmcat_version"] = "0.1.0"
    descriptor["id"] = f"{filename}"
    descriptor["description"] = f"{base_dir}"
    descriptor["catalog_file"] = f"{filename}.csv"
    descriptor["attributes"] = [
        {"column_name": x, "vocabulary": ""} for x in df.columns if x != "path"
    ]
    descriptor["assets"] = {"column_name": "path", "format": "netcdf"}

    with open(f"{filename}.json", "w") as json_file:
        json.dump(descriptor, json_file, indent=2)

    return df
