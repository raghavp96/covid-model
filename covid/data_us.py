"""
This module contains all US-specific data loading and data cleaning routines.
"""
import requests
import pandas as pd
import numpy as np

from .clean_us import process as clean_states

idx = pd.IndexSlice


def get_raw_covidtracking_data():
    """ Gets the current daily CSV from COVIDTracking """
    url = "https://covidtracking.com/api/v1/states/daily.csv"
    data = pd.read_csv(url)
    return data


def process_covidtracking_data(data: pd.DataFrame, run_date: pd.Timestamp):
    """ Processes raw COVIDTracking data to be in a form for the GenerativeModel.
        In many cases, we need to correct data errors or obvious outliers."""
    data = data.rename(columns={"state": "region"})
    data["date"] = pd.to_datetime(data["date"], format="%Y%m%d")
    data = data.set_index(["region", "date"]).sort_index()
    data = data[["positive", "total"]]

    # Too little data or unreliable reporting in the data source.
    data = data.drop(["MP", "GU", "AS", "PR", "VI"])

    # On Jun 5 Covidtracking started counting probable cases too
    # which increases the amount by 5014.
    # https://covidtracking.com/screenshots/MI/MI-20200605-184320.png
    data.loc[idx["MI", pd.Timestamp("2020-06-05") :], "positive"] -= 5014

    # From CT: On June 19th, LDH removed 1666 duplicate and non resident cases
    # after implementing a new de-duplicaton process.
    data.loc[idx["LA", pd.Timestamp("2020-06-19") :], :] += 1666

    # Now work with daily counts
    data = data.diff().dropna().clip(0, None).sort_index()

    # Note that when we set total to zero, the model ignores that date. See
    # the likelihood function in GenerativeModel.build

    # Clean data state-by-state
    data = clean_states(data, idx)

    # At the real time of `run_date`, the data for `run_date` is not yet available!
    # Cutting it away is important for backtesting!
    return data.loc[idx[:, :(run_date - pd.DateOffset(1))], ["positive", "total"]]


def get_and_process_covidtracking_data(run_date: pd.Timestamp):
    """ Helper function for getting and processing COVIDTracking data at once """
    data = get_raw_covidtracking_data()
    data = process_covidtracking_data(data, run_date)
    return data
