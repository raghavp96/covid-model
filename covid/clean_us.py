import pandas as pd

def process(data: pd.DataFrame, idx: pd.IndexSlice):
    state_processors = [
        process_US_MI,
        process_US_NJ,
        process_US_CA,
        process_US_SC,
        process_US_OR,
        process_US_OH,
        process_US_NV,
        process_US_WA,
        process_US_AL,
        process_US_AR,
        process_US_MS,
        process_US_PA
    ]

    for processor in state_processors:
        data = processor(data, idx)

    return data


def process_US_MI(data: pd.DataFrame, idx: pd.IndexSlice):
    # Michigan missed 6/18 totals and lumped them into 6/19 so we've
    # divided the totals in two and equally distributed to both days.
    data.loc[idx["MI", pd.Timestamp("2020-06-18")], "total"] = 14871
    data.loc[idx["MI", pd.Timestamp("2020-06-19")], "total"] = 14871

    return data


def process_US_NJ(data: pd.DataFrame, idx: pd.IndexSlice):
    # Huge outlier in NJ causing sampling issues.
    data.loc[idx["NJ", pd.Timestamp("2020-05-11")], :] = 0

    return data


def process_US_CA(data: pd.DataFrame, idx: pd.IndexSlice):
    # Huge outlier in CA causing sampling issues.
    data.loc[idx["CA", pd.Timestamp("2020-04-22")], :] = 0

    return data


def process_US_SC(data: pd.DataFrame, idx: pd.IndexSlice):
    # TODO: generally should handle when # tests == # positives and that
    # is not an indication of positive rate.
    data.loc[idx["SC", pd.Timestamp("2020-06-26")], :] = 0

    return data


def process_US_OR(data: pd.DataFrame, idx: pd.IndexSlice):
    # Two days of no new data then lumped sum on third day with lack of new total tests
    data.loc[idx["OR", pd.Timestamp("2020-06-26") : pd.Timestamp("2020-06-28")], 'positive'] = 174
    data.loc[idx["OR", pd.Timestamp("2020-06-26") : pd.Timestamp("2020-06-28")], 'total'] = 3296

    return data


def process_US_OH(data: pd.DataFrame, idx: pd.IndexSlice):
    # https://twitter.com/OHdeptofhealth/status/1278768987292209154
    data.loc[idx["OH", pd.Timestamp("2020-07-01")], :] = 0
    data.loc[idx["OH", pd.Timestamp("2020-07-09")], :] = 0

    return data


def process_US_NV(data: pd.DataFrame, idx: pd.IndexSlice):
    # Nevada didn't report total tests this day
    data.loc[idx["NV", pd.Timestamp("2020-07-02")], :] = 0

    return data


def process_US_WA(data: pd.DataFrame, idx: pd.IndexSlice):
    # A bunch of incorrect values for WA data so nulling them out.
    data.loc[idx["WA", pd.Timestamp("2020-06-05") : pd.Timestamp("2020-06-07")], :] = 0
    data.loc[idx["WA", pd.Timestamp("2020-06-20") : pd.Timestamp("2020-06-21")], :] = 0

    return data


def process_US_AL(data: pd.DataFrame, idx: pd.IndexSlice):
    # AL reported tests == positives
    data.loc[idx["AL", pd.Timestamp("2020-07-09")], :] = 0

    return data


def process_US_AR(data: pd.DataFrame, idx: pd.IndexSlice):
    # Low reported tests
    data.loc[idx["AR", pd.Timestamp("2020-07-10")], :] = 0

    return data


def process_US_MS(data: pd.DataFrame, idx: pd.IndexSlice):
    # Positives == tests
    data.loc[idx["MS", pd.Timestamp("2020-07-12")], :] = 0

    return data


def process_US_PA(data: pd.DataFrame, idx: pd.IndexSlice):
    # Outlier dates in PA
    data.loc[
        idx[
            "PA",
            [
                pd.Timestamp("2020-06-03"),
                pd.Timestamp("2020-04-21"),
                pd.Timestamp("2020-05-20"),
            ],
        ],
        :,
    ] = 0

    return data