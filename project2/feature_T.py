import numpy as np
import sys
import math
import tqdm
from feature_bookD import get_diff_count_units


_l_fn = {
    'power': math.pow,
    'log': math.log,
    'sqrt': math.sqrt,
    'raw': lambda x, y: x * y
}

def live_cal_T_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    ratio, level, interval, normal_fn = param
    # print(f'processing... {sys._getframe().f_code.co_name} {ratio}, level:{level}, decay:{interval}, normal_fn:{normal_fn}')

    # Function dictionary for normalization functions
    
    if normal_fn not in _l_fn:
        print('Error: normal_fn does not exist')
        exit(-1)
    
    decay = np.exp(-1.0 / interval)
    
    _flag = var.get('_flag', True)
    tradeIndicator = var.get('tradeIndicator', 0)
    
    if _flag:
        var['_flag'] = False
        return 0.0
    
    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = get_diff_count_units(diff)
    
    BSideTrade = _l_fn[normal_fn](ratio, _units_traded_1)
    ASideTrade = _l_fn[normal_fn](ratio, _units_traded_0)

    tradeIndicator += (-1 * BSideTrade + ASideTrade)
    var['tradeIndicator'] = tradeIndicator * decay
    
    return tradeIndicator

def live_cal_T_v2(param, gr_bid_level, gr_ask_level, diff, var, mid):
    ratio, level, interval, normal_fn = param
    # print(f'processing... {sys._getframe().f_code.co_name} {ratio}, level:{level}, decay:{interval}, normal_fn:{normal_fn}')
    
    if normal_fn not in _l_fn:
        print('Error: normal_fn does not exist')
        exit(-1)
    
    decay = np.exp(-1.0 / interval)
    
    _flag = var.get('_flag', True)
    tradeIndicator = var.get('tradeIndicator', 0)
    
    if _flag:
        var['_flag'] = False
        return 0.0

    mid_price = mid
    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = get_diff_count_units(diff)

    BSideTrade = _l_fn[normal_fn](ratio, _units_traded_1) * (_price_1)
    ASideTrade = _l_fn[normal_fn](ratio, _units_traded_0) * (_price_0)
    
    tradeIndicator += (-1 * BSideTrade + ASideTrade)
    var['tradeIndicator'] = tradeIndicator * decay
    
    return tradeIndicator
