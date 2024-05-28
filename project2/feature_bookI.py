import sys

def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    mid_price = mid
    ratio = param[0]
    level = param[1]
    interval = param[2]
    # print(f'processing... {sys._getframe().f_code.co_name} {ratio}, level:{level}, interval:{interval}')
    
    _flag = var.get('_flag', True)
    
    if _flag:  # skipping first line
        var['_flag'] = False
        return 0.0

    # Calculate the transformed quantities and their respective price impacts
    quant_v_bid = gr_bid_level['quantity']**ratio
    price_v_bid = gr_bid_level['price'] * quant_v_bid

    quant_v_ask = gr_ask_level['quantity']**ratio
    price_v_ask = gr_ask_level['price'] * quant_v_ask

    # Aggregating quantities and price impacts
    askQty = quant_v_ask.sum()
    bidPx = price_v_bid.sum()
    bidQty = quant_v_bid.sum()
    askPx = price_v_ask.sum()

    bid_ask_spread = interval  # this seems to be a static value passed as 'interval' - maybe the market spread?
    
    book_price = 0  # initialize to prevent division by zero
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty)) / (bidQty + askQty)

    indicator_value = (book_price - mid_price) / bid_ask_spread
    
    return indicator_value
