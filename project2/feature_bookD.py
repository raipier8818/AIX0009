import math
import sys

def live_cal_book_d_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    #print(gr_bid_level)
    #print(gr_ask_level)

    ratio = param[0]
    level = param[1]
    interval = param[2]
    # print(f'processing... {sys._getframe().f_code.co_name} {ratio}, level:{level}, interval:{interval}')

    decay = math.exp(-1.0 / interval)
    
    _flag = var.get('_flag', True)
    
    # Initializing variables if they are not set
    if 'prevBidQty' not in var:
        var['prevBidQty'] = var['prevAskQty'] = var['prevBidTop'] = var['prevAskTop'] = 0
        var['bidSideAdd'] = var['bidSideDelete'] = var['askSideAdd'] = var['askSideDelete'] = 0
        var['bidSideTrade'] = var['askSideTrade'] = var['bidSideFlip'] = var['askSideFlip'] = 0
        var['bidSideCount'] = var['askSideCount'] = 1  # avoid division by zero

    if _flag:
        var['prevBidQty'] = gr_bid_level['quantity'].sum()
        var['prevAskQty'] = gr_ask_level['quantity'].sum()
        var['prevBidTop'] = gr_bid_level.iloc[0]['price']
        var['prevAskTop'] = gr_ask_level.iloc[0]['price']
        var['_flag'] = False
        return 0.0

    curBidQty = gr_bid_level['quantity'].sum()
    curAskQty = gr_ask_level['quantity'].sum()
    curBidTop = gr_bid_level.iloc[0]['price']
    curAskTop = gr_ask_level.iloc[0]['price']

    update_trade_counters(var, curBidQty, curAskQty, curBidTop, curAskTop)

    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = get_diff_count_units(diff)

    var['bidSideTrade'] += _count_1
    var['bidSideCount'] += _count_1
    
    var['askSideTrade'] += _count_0
    var['askSideCount'] += _count_0
    
    bidBookV = calculate_volume(var['bidSideDelete'], var['bidSideAdd'], var['bidSideFlip'], var['bidSideCount'], ratio)
    askBookV = calculate_volume(var['askSideDelete'], var['askSideAdd'], var['askSideFlip'], var['askSideCount'], ratio)
    tradeV = (var['askSideTrade'] / var['askSideCount']**ratio) - (var['bidSideTrade'] / var['bidSideCount']**ratio)
    bookDIndicator = askBookV + bidBookV + tradeV

    apply_decay(var, decay)

    # Update previous values
    var['prevBidQty'] = curBidQty
    var['prevAskQty'] = curAskQty
    var['prevBidTop'] = curBidTop
    var['prevAskTop'] = curAskTop

    return bookDIndicator

def update_trade_counters(var, curBidQty, curAskQty, curBidTop, curAskTop):
    if curBidQty > var['prevBidQty']:
        var['bidSideAdd'] += 1
        var['bidSideCount'] += 1
    elif curBidQty < var['prevBidQty']:
        var['bidSideDelete'] += 1
        var['bidSideCount'] += 1
    if curAskQty > var['prevAskQty']:
        var['askSideAdd'] += 1
        var['askSideCount'] += 1
    elif curAskQty < var['prevAskQty']:
        var['askSideDelete'] += 1
        var['askSideCount'] += 1
        
    if curBidTop < var['prevBidTop']:
        var['bidSideFlip'] += 1
        var['bidSideCount'] += 1
    if curAskTop > var['prevAskTop']:
        var['askSideFlip'] += 1
        var['askSideCount'] += 1

def calculate_volume(deletes, adds, flips, count, ratio):
    return (-deletes + adds - flips) / (count ** ratio)

def apply_decay(var, decay):
    for key in ['bidSideCount', 'askSideCount', 'bidSideAdd', 'bidSideDelete',
                'askSideAdd', 'askSideDelete', 'bidSideTrade', 'askSideTrade',
                'bidSideFlip', 'askSideFlip']:
        var[key] *= decay

def get_diff_count_units (diff):
    
    _count_1 = _count_0 = _units_traded_1 = _units_traded_0 = 0
    _price_1 = _price_0 = 0

    diff_len = len (diff)
    if diff_len == 1:
        row = diff.iloc[0]
        if row['type'] == 1:
            _count_1 = row['count']
            _units_traded_1 = row['units_traded']
            _price_1 = row['price']
        else:
            _count_0 = row['count']
            _units_traded_0 = row['units_traded']
            _price_0 = row['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    elif diff_len == 2:
        row_1 = diff.iloc[1]
        row_0 = diff.iloc[0]
        _count_1 = row_1['count']
        _count_0 = row_0['count']

        _units_traded_1 = row_1['units_traded']
        _units_traded_0 = row_0['units_traded']
        
        _price_1 = row_1['price']
        _price_0 = row_0['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    # print('Error: diff length is not 1 or 2')
