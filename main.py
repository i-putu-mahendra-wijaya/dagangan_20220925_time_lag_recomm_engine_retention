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

user_trx = trx_histories[["trx_date", "count_sku", "user_id", "registration_date"]]

user_trx["prev_trx_date"] = (user_trx.groupby(trx_histories["user_id"]))["trx_date"].shift(1)
user_trx["registration_date"] = pd.to_datetime(user_trx["registration_date"], format="%Y-%m-%d")
user_trx["trx_date"] = pd.to_datetime(user_trx["trx_date"], format="%Y-%m-%d")
user_trx["prev_trx_date"]=pd.to_datetime(user_trx["prev_trx_date"], format="%Y-%m-%d")
user_trx["count_days_since_last_trx"] = (user_trx["trx_date"] - user_trx["prev_trx_date"]).dt.days
user_trx = user_trx.dropna()

user_trx["active_or_revival"] = np.where(user_trx["count_days_since_last_trx"] <= 30, "active", "revival")
user_trx["active_or_revival_numcat"] = np.where(user_trx["active_or_revival"] == "active", 1, 2)

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
only_ap_user["count_days_between_reg_and_active_period_start_date"] = (only_ap_user["active_period_start_date"] - only_ap_user["registration_date"]).dt.days
only_ap_user["active_period_end_date"] = oau_groupbyuser["trx_date"].max().to_list()
only_ap_user["today_date"] = list(itertools.repeat(
    date.today(),
    len(only_ap_user)
  ))
only_ap_user["today_date"] = pd.to_datetime(only_ap_user["today_date"], format="%Y-%m-%d")
only_ap_user["active_period_days"] = (only_ap_user["active_period_end_date"] - only_ap_user["active_period_start_date"]).dt.days
only_ap_user["last_trx_days_ago"] = (only_ap_user["today_date"] - only_ap_user["active_period_end_date"]).dt.days

# remove users that are not immediately transacting
only_ap_user = only_ap_user.drop(only_ap_user[only_ap_user.count_days_between_reg_and_active_period_start_date > 30].index)


only_ap_user["is_long_lived_user"] = np.where(
    (only_ap_user["active_period_days"] > 30) & (only_ap_user["last_trx_days_ago"] <= 30)
    , 1, 0
)

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
revived_user["count_days_between_reg_and_active_period_start_date"] = (revived_user["active_period_start_date"] - revived_user["registration_date"]).dt.days
revived_user["active_period_end_date"] = ru_groupbyuser["trx_date"].max().to_list()
revived_user["today_date"] = list(itertools.repeat(
    date.today(),
    len(revived_user)
  ))
revived_user["today_date"] = pd.to_datetime(revived_user["today_date"], format="%Y-%m-%d")
revived_user["active_period_days"] = (revived_user["active_period_end_date"] - revived_user["active_period_start_date"]).dt.days
revived_user["last_trx_days_ago"] = (revived_user["today_date"] - revived_user["active_period_end_date"]).dt.days

# remove users that are not immediately transacting
revived_user = revived_user.drop(revived_user[revived_user.count_days_between_reg_and_active_period_start_date > 30].index)


revived_user["is_long_lived_user"] = np.where(
    (revived_user["active_period_days"] > 30) & (revived_user["last_trx_days_ago"] <= 30)
        ,1, 0)

user_df = pd.concat([only_ap_user, revived_user], ignore_index=True).drop_duplicates()

user_filter = trx_histories["user_id"].isin(user_df["user_id"])

tmp_trx_histories = trx_histories[user_filter]

tmp_trx_histories["trx_date"] = pd.to_datetime(tmp_trx_histories["trx_date"], format="%Y-%m-%d")
tmp_trx_histories["registration_date"] = pd.to_datetime(tmp_trx_histories["registration_date"], format="%Y-%m-%d")

tmp_trx_histories["trx_week"] = tmp_trx_histories["trx_date"].dt.to_period("W")

tmp_user_df = user_df[["user_id", "is_long_lived_user"]]

tmp_trx_histories = tmp_trx_histories.merge(tmp_user_df, how="inner", on="user_id")

tth_groupbyweek = tmp_trx_histories.groupby("trx_week")

ts_count_sku = pd.DataFrame(
    columns=[
        "trx_week"
        , "avg_count_sku"
    ])

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