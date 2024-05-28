import pandas as pd
import timeit
import tqdm

from feature_bookI import *
from feature_bookD import *
from feature_mid import *
from feature_T import *


_l_indicator_fn = {
    "BI": live_cal_book_i_v1,
    "BDv1": live_cal_book_d_v1,
    # 'BDv2': live_cal_book_d_v2,
    # 'BDv3': live_cal_book_d_v3,
    "TIv1": live_cal_T_v1,
    "TIv2": live_cal_T_v2,
}

_l_indicator_name = {
    "BI": "book-imbalance",
    "BDv1": "book-delta-v1",
    # 'BDv2': 'book-delta-v2',
    # 'BDv3': 'book-delta-v3',
    "TIv1": "trade-indicator-v1",
    "TIv2": "trade-indicator-v2",
}


def get_sim_df(fn):
    print(f"loading... {fn}")
    df = pd.read_csv(fn).apply(pd.to_numeric, errors="ignore")

    # df["timestamp"] = pd.to_datetime(df["timestamp"])
    # df = df[df["timestamp"].between("2024-05-01 00:00:00", "2024-05-01 2:59:59")]

    group = df.groupby(["timestamp"])
    return group


def get_sim_df_trade(fn):
    print(f"loading... {fn}")
    df = pd.read_csv(fn).apply(pd.to_numeric, errors="ignore")
    group = df.groupby(["timestamp"])
    return group


def init_indicator_var(indicator, p):
    var = {}
    var["_flag"] = True
    if indicator in ["BI", "BDv1", "BDv2", "BDv3"]:
        var["prevBidQty"] = var["prevAskQty"] = var["prevBidTop"] = var[
            "prevAskTop"
        ] = 0
        var["bidSideAdd"] = var["bidSideDelete"] = var["askSideAdd"] = var[
            "askSideDelete"
        ] = 0
        var["bidSideTrade"] = var["askSideTrade"] = var["bidSideFlip"] = var[
            "askSideFlip"
        ] = 0
        var["bidSideCount"] = var["askSideCount"] = 1  # avoid division by zero
    elif indicator in ["TIv1", "TIv2"]:
        var["tradeIndicator"] = 0

    return var


def add_norm_fn(params):
    return [(p[0], p[1], p[2], "raw") for p in params]


def wrong_trade_time_diff(gr_t):
    if len(gr_t) < 2:
        return False

    t1 = gr_t.iloc[0]["timestamp"]
    t2 = gr_t.iloc[1]["timestamp"]
    if t1 == t2:
        return False

    return True


def agg_order_book(gr_o, timestamp):
    gr_o = gr_o.groupby(["price", "type"]).agg({"quantity": "sum"}).reset_index()
    gr_o["timestamp"] = timestamp
    return gr_o


def faster_calc_indicators(raw_fn):
    start_time = timeit.default_timer()

    orderbook_filename = "2024-05-01-upbit-BTC-book.csv"
    trade_filename = "2024-05-01-upbit-BTC-trade.csv"

    # FROM CSV FILES (DAILY)
    group_o = get_sim_df(orderbook_filename)
    group_t = get_sim_df_trade(trade_filename)  # fix for book-1 regression

    delay = timeit.default_timer() - start_time
    print(f"df loading delay: {delay:.2f}s")

    level_1 = 2
    level_2 = 5

    # print('param levels', exchange, currency, level_1, level_2)

    book_imbalance_params = [(0.2, level_1, 1), (0.2, level_2, 1)]
    book_delta_params = [
        (0.2, level_1, 1),
        (0.2, level_1, 5),
        (0.2, level_1, 15),
        (0.2, level_2, 1),
        (0.2, level_2, 5),
        (0.2, level_2, 15),
    ]
    trade_indicator_params = [(0.2, level_1, 1), (0.2, level_1, 5), (0.2, level_1, 15)]

    variables = {}
    _dict = {}
    _dict_indicators = {}

    for p in book_imbalance_params:
        indicator = "BI"
        _dict.update({(indicator, p): []})
        _dict_var = init_indicator_var(indicator, p)
        variables.update({(indicator, p): _dict_var})

    for p in book_delta_params:
        for indicator_version in ["BDv1"]:
            _dict.update({(indicator_version, p): []})
            _dict_var = init_indicator_var(indicator_version, p)
            variables.update({(indicator_version, p): _dict_var})

    for p in add_norm_fn(trade_indicator_params):
        for indicator_version in ["TIv1", "TIv2"]:
            _dict.update({(indicator_version, p): []})
            _dict_var = init_indicator_var(indicator_version, p)
            variables.update({(indicator_version, p): _dict_var})

    _timestamp = []
    _mid_price = []

    print("total groups:", len(group_o.size().index), len(group_t.size().index))
    banded = False

    sz = len(group_o.size().index)
    seq = 0

    for gr_o, gr_t in tqdm.tqdm(
        zip(group_o, group_t), desc="feature calculation", total=sz
    ):
        if gr_o is None or gr_t is None:
            print("Warning: group is empty")
            continue

        if wrong_trade_time_diff(gr_t[1]):
            continue

        timestamp = (gr_o[1].iloc[0])["timestamp"]
        # print(timestamp)
        # break

        if banded:
            gr_o = agg_order_book(gr_o[1], timestamp)
            gr_o = gr_o.reset_index()
            del gr_o["index"]
        else:
            gr_o = gr_o[1]

        gr_t = gr_t[1]

        gr_bid_level = gr_o[(gr_o.type == 0)]
        gr_ask_level = gr_o[(gr_o.type == 1)]
        diff = gr_t

        mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(
            gr_bid_level, gr_ask_level, gr_t
        )

        if bid >= ask:
            seq += 1
            continue

        _timestamp.append(timestamp)
        _mid_price.append(mid_price)

        _dict_group = {}
        for indicator, p in _dict.keys():
            level = p[1]
            if level not in _dict_group:
                orig_level = level
                level = min(level, len(gr_bid_level), len(gr_ask_level))
                _dict_group[level] = (
                    gr_bid_level.head(level),
                    gr_ask_level.head(level),
                )

            p1 = ()
            if len(p) == 3:
                p1 = (p[0], level, p[2])
            if len(p) == 4:
                p1 = (p[0], level, p[2], p[3])

            _i = _l_indicator_fn[indicator](
                p1,
                _dict_group[level][0],
                _dict_group[level][1],
                diff,
                variables[(indicator, p)],
                mid_price,
            )
            _dict[(indicator, p)].append(_i)

        for indicator, p in _dict.keys():
            col_name = (
                f'{_l_indicator_name[indicator].replace("_", "-")}-{p[0]}-{p[1]}-{p[2]}'
            )
            if indicator in ["TIv1", "TIv2"]:
                col_name = f'{_l_indicator_name[indicator].replace("_", "-")}-{p[0]}-{p[1]}-{p[2]}-{p[3]}'
            _dict_indicators[col_name] = _dict[(indicator, p)]

        _dict_indicators["timestamp"] = _timestamp
        _dict_indicators["mid_price"] = _mid_price

        # print(_dict_indicators['timestamp'])
        # break

        seq += 1

        # update tqdm progress bar

    fn = indicators_csv(raw_fn)
    df_dict_to_csv(_dict_indicators, fn)


def indicators_csv(raw_fn, hour=None):
    if hour == None:
        return f"{raw_fn}.csv"
    else:
        return f"{raw_fn}-{hour}.csv"


def df_dict_to_csv(df_dict, fn, start=None, end=None):
    df = pd.DataFrame(df_dict)

    if start != None and end != None:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"].between(start, end)]

    df.to_csv(fn, index=False)
    print(f"writing... {fn}")
