import os
import pandas as pd

pd.options.mode.chained_assignment = None

from datetime import date
import itertools
import numpy as np
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader
from project_custom_package.BigQueryFetcher import BigQueryFetcher
import matplotlib.pyplot as plt

# load env variables
load_dotenv()

# load model configuration
with open("./model_config.yaml") as ref:
    model_config = yaml.load(ref, Loader=SafeLoader)
    print(model_config)

query_directory_path = model_config["QUERY_DIRECTORY_PATH"]


def fetch_trx_histories_from_gbq() -> pd.DataFrame:
    trx_histories_sql_path = os.path.join(query_directory_path, "trx_histories.sql")

    with open(trx_histories_sql_path) as query_file:
        trx_histories_sql = query_file.read()

    trx_histories_query_string = f"""{trx_histories_sql}"""
    trx_histories_bqclient = BigQueryFetcher(trx_histories_query_string)
    trx_histories = trx_histories_bqclient.fetch_data()

    return trx_histories


trx_histories = fetch_trx_histories_from_gbq()


def fetch_sales_visit_histories_from_gbq():
    sales_visi_histories_sql_path = os.path.join(query_directory_path, "sales_visit_histories.sql")

    with open(sales_visi_histories_sql_path) as query_file:
        sales_visit_histories_sql = query_file.read()

    sales_visit_histories_query_string = f"""{sales_visit_histories_sql}"""
    sales_visit_histories_bqclient = BigQueryFetcher(sales_visit_histories_query_string)
    sales_visit_histories = sales_visit_histories_bqclient.fetch_data()

    return sales_visit_histories


sales_visit_histories = fetch_sales_visit_histories_from_gbq()


def create_user_trx_table(trx_histories: pd.DataFrame) -> pd.DataFrame:
    user_trx = trx_histories[["trx_date", "count_sku", "user_id", "registration_date"]]

    user_trx["prev_trx_date"] = (user_trx.groupby(trx_histories["user_id"]))["trx_date"].shift(1)
    user_trx["registration_date"] = pd.to_datetime(user_trx["registration_date"], format="%Y-%m-%d")
    user_trx["trx_date"] = pd.to_datetime(user_trx["trx_date"], format="%Y-%m-%d")
    user_trx["prev_trx_date"] = pd.to_datetime(user_trx["prev_trx_date"], format="%Y-%m-%d")
    user_trx["count_days_since_last_trx"] = (user_trx["trx_date"] - user_trx["prev_trx_date"]).dt.days
    user_trx = user_trx.dropna()

    user_trx["active_or_revival"] = np.where(user_trx["count_days_since_last_trx"] <= 30, "active", "revival")
    user_trx["active_or_revival_numcat"] = np.where(user_trx["active_or_revival"] == "active", 1, 2)

    return user_trx


user_trx = create_user_trx_table(trx_histories=trx_histories)


def create_only_active_period_users_table(user_trx: pd.DataFrame) -> pd.DataFrame:
    utx_groupbyuser = user_trx.groupby("user_id")
    utx_only_ap_user = utx_groupbyuser.filter(lambda x: x["active_or_revival_numcat"].max() == 1)

    only_ap_user = pd.DataFrame(
        columns=[
            "user_id"
            , "registration_date"
            , "count_days_between_reg_and_active_period_start_date"
            , "active_period_start_date"
            , "active_period_end_date"
            , "active_period_days"
            , "today_date"
            , "last_trx_days_ago"
        ])

    oau_groupbyuser = utx_only_ap_user.groupby("user_id")

    only_ap_user["user_id"] = oau_groupbyuser["user_id"].unique().to_list()
    only_ap_user["user_id"] = only_ap_user["user_id"].apply(lambda x: x[0])
    only_ap_user["registration_date"] = oau_groupbyuser["registration_date"].unique().to_list()
    only_ap_user["registration_date"] = only_ap_user["registration_date"].apply(lambda x: x[0])
    only_ap_user["active_period_start_date"] = oau_groupbyuser["prev_trx_date"].min().to_list()
    only_ap_user["count_days_between_reg_and_active_period_start_date"] = (
            only_ap_user["active_period_start_date"] - only_ap_user["registration_date"]).dt.days
    only_ap_user["active_period_end_date"] = oau_groupbyuser["trx_date"].max().to_list()
    only_ap_user["today_date"] = list(itertools.repeat(
        date.today(),
        len(only_ap_user)
    ))
    only_ap_user["today_date"] = pd.to_datetime(only_ap_user["today_date"], format="%Y-%m-%d")
    only_ap_user["active_period_days"] = (
            only_ap_user["active_period_end_date"] - only_ap_user["active_period_start_date"]).dt.days
    only_ap_user["last_trx_days_ago"] = (only_ap_user["today_date"] - only_ap_user["active_period_end_date"]).dt.days

    # remove users that are not immediately transacting
    only_ap_user = only_ap_user.drop(
        only_ap_user[only_ap_user.count_days_between_reg_and_active_period_start_date > 30].index)

    only_ap_user["is_long_lived_user"] = np.where(
        (only_ap_user["active_period_days"] > 30) & (only_ap_user["last_trx_days_ago"] <= 30)
        , 1, 0
    )

    return only_ap_user


only_ap_user = create_only_active_period_users_table(user_trx=user_trx)


def create_revived_users_table(user_trx: pd.DataFrame) -> pd.DataFrame:
    utx_groupbyuser = user_trx.groupby("user_id")

    utx_revived_users = utx_groupbyuser.filter(lambda x: x["active_or_revival_numcat"].max() == 2)

    revived_user = pd.DataFrame(
        columns=[
            "user_id"
            , "registration_date"
            , "count_days_between_reg_and_active_period_start_date"
            , "active_period_start_date"
            , "active_period_end_date"
            , "active_period_days"
            , "today_date"
            , "last_trx_days_ago"
        ])

    ru_groupbyuser = utx_revived_users.groupby("user_id")

    revived_user["user_id"] = ru_groupbyuser["user_id"].unique().to_list()
    revived_user["user_id"] = revived_user["user_id"].apply(lambda x: x[0])
    revived_user["registration_date"] = ru_groupbyuser["registration_date"].unique().to_list()
    revived_user["registration_date"] = revived_user["registration_date"].apply(lambda x: x[0])
    revived_user["active_period_start_date"] = ru_groupbyuser["prev_trx_date"].min().to_list()
    revived_user["count_days_between_reg_and_active_period_start_date"] = (
            revived_user["active_period_start_date"] - revived_user["registration_date"]).dt.days
    revived_user["active_period_end_date"] = ru_groupbyuser["trx_date"].max().to_list()
    revived_user["today_date"] = list(itertools.repeat(
        date.today(),
        len(revived_user)
    ))
    revived_user["today_date"] = pd.to_datetime(revived_user["today_date"], format="%Y-%m-%d")
    revived_user["active_period_days"] = (
            revived_user["active_period_end_date"] - revived_user["active_period_start_date"]).dt.days
    revived_user["last_trx_days_ago"] = (revived_user["today_date"] - revived_user["active_period_end_date"]).dt.days

    # remove users that are not immediately transacting
    revived_user = revived_user.drop(
        revived_user[revived_user.count_days_between_reg_and_active_period_start_date > 30].index)

    revived_user["is_long_lived_user"] = np.where(
        (revived_user["active_period_days"] > 30) & (revived_user["last_trx_days_ago"] <= 30)
        , 1, 0)

    return revived_user


revived_user = create_revived_users_table(user_trx=user_trx)


def union_ap_user_and_revived_user(only_ap_user: pd.DataFrame, revived_user: pd.DataFrame) -> pd.DataFrame:
    user_df = pd.concat([only_ap_user, revived_user], ignore_index=True).drop_duplicates()

    return user_df


user_df = union_ap_user_and_revived_user(only_ap_user=only_ap_user, revived_user=revived_user)


def filter_trx_histories(trx_histories: pd.DataFrame, user_df: pd.DataFrame) -> pd.DataFrame:
    user_filter = trx_histories["user_id"].isin(user_df["user_id"])

    tmp_trx_histories = trx_histories[user_filter]

    tmp_trx_histories["trx_date"] = pd.to_datetime(tmp_trx_histories["trx_date"], format="%Y-%m-%d")
    tmp_trx_histories["registration_date"] = pd.to_datetime(tmp_trx_histories["registration_date"], format="%Y-%m-%d")

    tmp_trx_histories["trx_week"] = tmp_trx_histories["trx_date"].dt.to_period("W")

    tmp_user_df = user_df[["user_id", "is_long_lived_user"]]

    tmp_trx_histories = tmp_trx_histories.merge(tmp_user_df, how="inner", on="user_id")

    return tmp_trx_histories


tmp_trx_histories = filter_trx_histories(trx_histories=trx_histories, user_df=user_df)


def filter_sales_visit_histories(sales_visit_histories: pd.DataFrame, user_df: pd.DataFrame) -> pd.DataFrame:
    user_filter = sales_visit_histories["user_id"].isin(user_df["user_id"])

    tmp_sales_visit_histories = sales_visit_histories[user_filter]

    tmp_sales_visit_histories["visit_date"] = pd.to_datetime(tmp_sales_visit_histories["visit_date"], format="%Y-%m-%d")

    tmp_sales_visit_histories["visit_week"] = tmp_sales_visit_histories["visit_date"].dt.to_period("W")

    return tmp_sales_visit_histories


tmp_sales_visit_histories = filter_sales_visit_histories(sales_visit_histories=sales_visit_histories, user_df=user_df)


def create_time_series_count_sku(tmp_trx_histories: pd.DataFrame) -> pd.DataFrame:
    tth_groupbyweek = tmp_trx_histories.groupby("trx_week")

    def calc_avg_count_sku(df_block):
        avg_count_sku = df_block["count_sku"].mean()
        avg_count_sku = int(avg_count_sku)
        return avg_count_sku

    ts_count_sku = tth_groupbyweek.apply(
        lambda x: pd.Series({
            "avg_count_sku": calc_avg_count_sku(x)
        })
    )

    ts_count_sku.reset_index(inplace=True)

    return ts_count_sku


ts_count_sku = create_time_series_count_sku(tmp_trx_histories=tmp_trx_histories)


def create_time_series_count_visit(tmp_sales_visit_histories: pd.DataFrame) -> pd.DataFrame:
    tsv_groupbyweek = tmp_sales_visit_histories.groupby("visit_week")

    def calc_count_visit(df_block: pd.DataFrame):
        count_visit = len(df_block["user_id"].unique())
        return count_visit

    ts_count_visit = tsv_groupbyweek.apply(
        lambda x: pd.Series({
            "count_visit": calc_count_visit(x)
        })
    )

    ts_count_visit.reset_index(inplace=True)

    return ts_count_visit


ts_count_visit = create_time_series_count_visit(tmp_sales_visit_histories=tmp_sales_visit_histories)


def create_time_series_llu(tmp_trx_histories: pd.DataFrame) -> pd.DataFrame:
    tmp_trx_from_llu = tmp_trx_histories[tmp_trx_histories["is_long_lived_user"] == 1]

    tth_groupbyweek = tmp_trx_from_llu.groupby("trx_week")

    def calc_count_long_lived_user(df_block):
        list_long_lived_users = df_block["user_id"].unique()
        count_long_lived_user = len(list_long_lived_users)
        return count_long_lived_user

    ts_count_llu = tth_groupbyweek.apply(
        lambda x: pd.Series({
            "count_long_lived_user": calc_count_long_lived_user(x)
        })
    )

    ts_count_llu.reset_index(inplace=True)

    return ts_count_llu


ts_count_llu = create_time_series_llu(tmp_trx_histories=tmp_trx_histories)

ts_count_llu_for_cvs = ts_count_llu.copy()
ts_count_llu_for_cvs.rename(columns={
    "trx_week": "visit_week"
}, inplace=True)

ts_sku_llu = ts_count_sku.merge(ts_count_llu, how="inner", on="trx_week")

ts_cvs_llu = ts_count_visit.merge(ts_count_llu_for_cvs, how="inner", on="visit_week")


def normalize_ts_sku_llu(ts_sku_llu: pd.DataFrame) -> pd.DataFrame:
    def convert_period_to_timestamp(each_period):
        corres_timestamp = each_period.end_time

        return corres_timestamp

    ts_sku_llu["week_end_time"] = ts_sku_llu["trx_week"].apply(lambda x: convert_period_to_timestamp(x))
    ts_sku_llu.drop(["trx_week"], axis=1)
    ts_sku_llu = ts_sku_llu[["week_end_time", "avg_count_sku", "count_long_lived_user"]]
    ts_sku_llu = ts_sku_llu.rename(columns={"week_end_time": "trx_week"})

    tsl_minmax_norm = ts_sku_llu.copy()

    for col in ["avg_count_sku", "count_long_lived_user"]:
        tsl_minmax_norm[col] = (tsl_minmax_norm[col] - tsl_minmax_norm[col].min()) / (
                tsl_minmax_norm[col].max() - tsl_minmax_norm[col].min())

    # apply exponential-weighted mean to smoothen tsl_minmax_normalized, windows size = 10 period
    tsl_minmax_norm["avg_count_sku"] = tsl_minmax_norm["avg_count_sku"].ewm(span=10).mean()
    tsl_minmax_norm["count_long_lived_user"] = tsl_minmax_norm["count_long_lived_user"].ewm(span=10).mean()

    return tsl_minmax_norm


tsl_minmax_norm = normalize_ts_sku_llu(ts_sku_llu=ts_sku_llu)


def normalize_ts_cvs_llu(ts_cvs_llu: pd.DataFrame) -> pd.DataFrame:
    def convert_period_to_timestamp(each_period):
        corres_timestamp = each_period.end_time

        return corres_timestamp

    ts_cvs_llu["week_end_time"] = ts_cvs_llu["visit_week"].apply(lambda x: convert_period_to_timestamp(x))
    ts_cvs_llu.drop(["visit_week"], axis=1)
    ts_cvs_llu = ts_cvs_llu[["week_end_time", "count_visit", "count_long_lived_user"]]
    ts_cvs_llu = ts_cvs_llu.rename(columns={"week_end_time": "visit_week"})

    tsl_minmax_norm = ts_cvs_llu.copy()

    for col in ["count_visit", "count_long_lived_user"]:
        tsl_minmax_norm[col] = (tsl_minmax_norm[col] - tsl_minmax_norm[col].min()) / (
                tsl_minmax_norm[col].max() - tsl_minmax_norm[col].min())

    # apply exponential-weighted mean to smoothen tsl_minmax_normalized, windows size = 10 period
    tsl_minmax_norm["count_visit"] = tsl_minmax_norm["count_visit"].ewm(span=10).mean()
    tsl_minmax_norm["count_long_lived_user"] = tsl_minmax_norm["count_long_lived_user"].ewm(span=10).mean()

    return tsl_minmax_norm


tcl_minmax_norm = normalize_ts_cvs_llu(ts_cvs_llu=ts_cvs_llu)


def plot_ts_sku_llu(ts_sku_llu: pd.DataFrame):
    fig, ax1 = plt.subplots()

    ax1_color = "#E68D44"

    ax1.plot(ts_sku_llu["trx_week"],
             ts_sku_llu["avg_count_sku"],
             color=ax1_color)

    ax1.set_xlabel("trx_week")
    ax1.set_ylabel("avg_count_sku", color=ax1_color)
    ax1.tick_params(axis="y", labelcolor=ax1_color)

    ax2 = ax1.twinx()

    ax2_color = "#1889F0"

    ax2.plot(ts_sku_llu["trx_week"],
             ts_sku_llu["count_long_lived_user"],
             color=ax2_color)

    ax2.set_ylabel("count_long_lived_user", color=ax2_color)
    ax2.tick_params(axis="y", labelcolor=ax2_color)
    plt.show()

    # save png image to current folder
    # fig.savefig("time series count sku & count long-lived user.png",
    #             format="png")


# plot_ts_sku_llu(ts_sku_llu=tsl_minmax_norm)

def plot_ts_cvs_llu(ts_cvs_llu: pd.DataFrame) -> pd.DataFrame:
    fig, ax1 = plt.subplots()

    ax1_color = "#E68D44"

    ax1.plot(ts_cvs_llu["visit_week"],
             ts_cvs_llu["count_visit"],
             color=ax1_color)

    ax1.set_xlabel("visit_week")
    ax1.set_ylabel("count_visit", color=ax1_color)
    ax1.tick_params(axis="y", labelcolor=ax1_color)

    ax2 = ax1.twinx()

    ax2_color = "#1889F0"

    ax2.plot(ts_cvs_llu["visit_week"],
             ts_cvs_llu["count_long_lived_user"],
             color=ax2_color)

    ax2.set_ylabel("count_long_lived_user", color=ax2_color)
    ax2.tick_params(axis="y", labelcolor=ax2_color)
    plt.show()


# plot_ts_cvs_llu(ts_cvs_llu=tcl_minmax_norm)


# calculate Time-Lagged Cross Correlation (TLCC)

def crosscorr(datax, datay, lag=0, wrap=False):
    """ Lag-N cross correlation.
    Shifted data filled with NaNs

    Parameters
    ----------
    lag : int, default 0
    datax, datay : pandas.Series objects of equal length
    Returns
    ----------
    crosscorr : float
    """
    if wrap:
        shiftedy = datay.shift(lag)
        shiftedy.iloc[:lag] = datay.iloc[-lag:].values
        return datax.corr(shiftedy)
    else:
        return datax.corr(datay.shift(lag))


correl_window = range(0, len(tsl_minmax_norm))

rs_count_sku = [
    crosscorr(
        datax=tsl_minmax_norm["avg_count_sku"],
        datay=tsl_minmax_norm["count_long_lived_user"],
        lag=lag)
    for lag in correl_window
]

rs_count_visit = [
    crosscorr(
        datax=tcl_minmax_norm["count_long_lived_user"],
        datay=tcl_minmax_norm["count_visit"],
        lag=lag
    )
    for lag in correl_window
]

ts_shift = [len(tsl_minmax_norm) - lag for lag in correl_window]

tlcc_df = pd.DataFrame(data={
    "ts_shift": ts_shift,
    "cross_correl_count_sku": rs_count_sku,
    "cross_correl_count_visit": rs_count_visit
})


def plot_tlcc(tlcc_df: pd.DataFrame):
    fig, ax1 = plt.subplots(nrows=1, ncols=1)

    ax1_color = "#E68D44"
    ax1.plot("ts_shift", "cross_correl_count_sku", data=tlcc_df, color=ax1_color)
    ax1.set_xlabel("time shift (week)")
    ax1.set_ylabel("cross correl count sku", color=ax1_color)
    ax1.axes.yaxis.set_ticklabels([])
    ax1.tick_params(axis="y", labelcolor=ax1_color)

    ax2_color = "#1889F0"

    ax2 = ax1.twinx()

    ax2.plot("ts_shift", "cross_correl_count_visit", data=tlcc_df, color=ax2_color)

    ax2.set_ylabel("cross_correl_count_visitr", color=ax2_color)
    ax2.axes.yaxis.set_ticklabels([])
    ax2.tick_params(axis="y", labelcolor=ax2_color)

    plt.title(label="Time-Lagged Cross Correlation (TLCC) to count long-lived user", y=1.05)

    plt.axvline(x=20, color="#1889F0", linestyle="--", alpha=0.5, label="sigf correl count visit")
    plt.axvline(x=35, color="#E68D44", linestyle="--", alpha=0.5, label="sigf correl count sku")

    plt.show()


plot_tlcc(tlcc_df=tlcc_df)
