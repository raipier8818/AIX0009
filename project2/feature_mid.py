import math

def cal_mid_price(gr_bid_level, gr_ask_level, group_t, mid_type='wt'):
    level = 5
    
    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0]['price']
        bid_top_level_qty = gr_bid_level.iloc[0]['quantity']
        ask_top_price = gr_ask_level.iloc[0]['price']
        ask_top_level_qty = gr_ask_level.iloc[0]['quantity']
        mid_price = (bid_top_price + ask_top_price) / 2  # Basic mid price calculation

        if mid_type == 'wt':
            # Weighted mid price using top 'level' prices
            mid_price = ((gr_bid_level.head(level)['price'].mean() +
                          gr_ask_level.head(level)['price'].mean()) / 2)
        elif mid_type == 'mkt':
            # Market mid price based on top bid and ask quantities
            mid_price = ((bid_top_price * ask_top_level_qty + ask_top_price * bid_top_level_qty) /
                         (bid_top_level_qty + ask_top_level_qty))
            mid_price = truncate(mid_price, 1)
        elif mid_type == 'vwap':
            # Volume Weighted Average Price from trade data
            if group_t['units_traded'].sum() != 0:
                mid_price = group_t['total'].sum() / group_t['units_traded'].sum()
                mid_price = truncate(mid_price, 1)

        # print(mid_type, mid_price)
        return (mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty)

    else:
        print('Error: insufficient data for cal_mid_price')
        return (-1, -1, -1, -1, -1)

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

