import time
from datetime import datetime
import requests
import pandas as pd
import threading
import sys
import os
import logging

def get_bithumb_market_list() -> list[str]:
    url = 'https://api.bithumb.com/public/orderbook/ALL_KRW/' 
    response = requests.get(url)
    if response.status_code != 200:
        # print("Error! status code : ", response.status_code)
        logger.error(f"Error! {url} response status code : {response.status_code}")
        sys.exit(0)
    if response.json()["status"] != "0000":
        # print("Error! response status : ", response.json()["status"])
        logger.error(f"Error! {url} response status : {response.json()['status']}")
        sys.exit(0)
    
    return list(response.json()["data"].keys())

def get_bithumb_url(market : str, level: int) -> str:
    url = 'https://api.bithumb.com/public/orderbook/' + market
    query = {'count': str(level)}
    query = '?' + '&'.join(list(map(lambda x : x + '=' + query[x], [i for i in query])))
    return url + query

def get_bidask(bids, asks, timestamp) -> dict:    
    bids_type = 0
    asks_type = 1
    
    bids = pd.DataFrame(bids, columns=["price", "quantity", "type", "timestamp"])
    bids["type"] = bids_type
    bids["timestamp"] = timestamp

    asks = pd.DataFrame(asks, columns=["price", "quantity", "type", "timestamp"])
    asks["type"] = asks_type
    asks["timestamp"] = timestamp
    
    return {"bids": bids, "asks": asks}
    

def get_orderbook(market: str, level: int) -> pd.DataFrame:
    # get url using market and level
    url = get_bithumb_url(market, level)    
    # get response from url
    response = requests.get(url)
    if response.status_code != 200:
        # print("Error :", response.status_code)
        logger.error(f"Error! {url} response status code : {response.status_code}")
        return None
        
    # get timestamp, bids, asks from response
    timestamp = response.json()["data"]["timestamp"]
    timestamp = str(datetime.fromtimestamp(int(timestamp[:-3]))) + '.' + timestamp[-3:]
    bids = response.json()["data"]["bids"]
    asks = response.json()["data"]["asks"]

    result = get_bidask(bids, asks, timestamp)
    result = pd.concat([result["bids"], result["asks"]]).reset_index(drop=True)
    # print(result)
    return result

def save(df: pd.DataFrame, filename: str) -> None:
    if not os.path.exists(filename):
        df.to_csv(filename, index=False, mode='w')
    else:
        df.to_csv(filename, index=False, mode='a', header=False)
        

def thread(market: str, duration: int, interval: int):
    thread_id = threading.get_native_id()
    
    if test == False:
        target_time = "00:00:00"
        # print(f"Waiting for {target_time}...")
        logger.info(f"Thread {thread_id} waiting for {target_time} to start...")
        while(1):
            current_time = time.strftime("%H:%M:%S")
            if current_time == target_time:
                break
        print("Thread started for market :", market)
    else:
        # print("Test mode")
        logger.info(f"Thread {thread_id} started for market : {market} (Test mode)")
    
    while(duration > 0):
        current_date = time.strftime("%Y-%m-%d")
        filename = f"./orderbook/book-{current_date}-bithumb-{market.lower()}.csv"
        df = get_orderbook(market, 5)
        if df is None:
            logger.error(f"Thread {thread_id} : Error occured while getting orderbook data.")
            continue
        save(df, filename)
        
        # print(f"Thread {thread_id} : Saved {filename} at {time.strftime('%H:%M:%S')}")
        logger.info(f"Thread {thread_id} : Saved {filename} at {time.strftime('%H:%M:%S')}")
        
        time.sleep(interval)
        duration -= interval
        
    logger.info(f"Thread {thread_id} : Finished for market : {market}")

def main(market: list[str], duration: int, interval: int):
    # set logger
    global logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s | %(levelname)s ] %(message)s')    
    # log file
    current_date = time.strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(f"orderbook-{current_date}.log")
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    threads = []
    for m in market:
        th = threading.Thread(target=thread, args=(m, duration, interval))
        th.daemon = True
        th.start()
        threads.append(th)
        
    for th in threads:
        th.join()

    # print("All threads are finished.")
    logger.info("All threads are finished.")
    
def get_args(args: list[str]) -> dict:
    # --market="market1,market2,market3" --duration=dt --interval=it
    # -m="market1,market2,market3" -d=dt -i=it
    # --list
    # -l
    if len(args) <= 1:
        arg = "--help"
    else:
        arg = args[1]
        
    if arg.startswith("--help") or arg.startswith("-h"):
        # print command help
        
        print('Command: python orderbook.py --market=market1,market2,market3... --duration=dt --interval=it --test')
        print('Command: python orderbook.py -m=market1,market2,market3... -d=dt -i=it -t')
        print('Command: python orderbook.py --list')
        print('Command: python orderbook.py -l')
        print("\t--help,     -h: print help message")
        print("\t--market,   -m: market name list separated by comma (no space)")
        print("\t--duration, -d: duration time 'dt' in seconds. default is 1 hour.")
        print("\t--interval, -i: interval time 'it' in seconds. default is 1 second.")
        print("\t--test,     -t: test mode (for debugging)")
        print("\t--list,     -l: show available market list")
        sys.exit(0)
        
    if arg.startswith("--list") or arg.startswith("-l"):
        market_list = "\t".join(get_bithumb_market_list()[2:])
        print("Available market list:")
        print(market_list)
        sys.exit(0)

    global test
    test = False
    market = []
    duration = 0
    interval = 0
    
    for i in range(1, len(args)):
        arg = args[i]
        if arg.startswith("--market=") or arg.startswith("-m="):
            market = arg.split("=")[1].split(",")
        elif arg.startswith("--duration=") or arg.startswith("-d="):
            try:
                duration = int(arg.split("=")[1])
            except:
                print("Invalid argument. Duration time should be integer.")
                sys.exit(0)
        elif arg.startswith("--interval=") or arg.startswith("-i="):
            try:
                interval = int(arg.split("=")[1])
            except:
                print("Invalid argument. Interval time should be integer.")
                sys.exit(0)
        elif arg.startswith("--test") or arg.startswith("-t"):
            test = True
        else:
            print("Invalid argument. Please use --help or -h for help.")
            sys.exit(0)        
            
    
    if len(market) == 0:
        print("Invalid argument. Market name is required.")
        sys.exit(0)
    if duration < 0:
        print("Invalid argument. Duration time is positive.")
        sys.exit(0)
    if interval < 0:
        print("Invalid argument. Interval time is positive.")
        sys.exit(0)
    
    market_list = get_bithumb_market_list()
    for m in market:
        if m not in market_list:
            print(f"Invalid argument. {m} is not in market list.")
            sys.exit(0)
            
    if duration < interval:
        print("Invalid argument. Duration time should be greater than interval time.")
        sys.exit(0)
    
    if duration == 0:
        print("Setting default duration time to 1 hour.")
        duration = 3600
    
    if interval == 0:
        print("Setting default interval time to 1 second.")
        interval = 1
    
    
    args = {"market": market, "duration": duration, "interval": interval}
    return args
    
    

if __name__ == "__main__":
    args = get_args(sys.argv)
    # check orderbook directory exists
    if not os.path.exists("./orderbook"):
        os.makedirs("./orderbook")
    main(args["market"], args["duration"], args["interval"])