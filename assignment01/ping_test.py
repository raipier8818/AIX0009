import datetime
import subprocess
import time
from threading import Thread
import json
import pandas
import platform
from tqdm import tqdm
import sys

def current_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ping(host):
    timestamp = current_timestamp()
    p = None
    host_ip = None
    ping_size = None
    t = None
    ttl = None

    if platform.system() == "Darwin": # Mac
        try:
            p = subprocess.run(["ping", "-c", "1", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            result = p.stdout.split("\n")
            host_ip = result[0].split(" ")[2][1:-2]
            ping_size = result[1].split(" ")[0]
            t = result[1].split(" ")[6].split("=")[1]
            ttl = result[1].split(" ")[5].split("=")[1]
            min_t = result[5].split(" ")[3].split("/")[0]
            avg_t = result[5].split(" ")[3].split("/")[1]
            max_t = result[5].split(" ")[3].split("/")[2]
            stddev_t = result[5].split(" ")[3].split("/")[3]
        except:
            host_ip = "0.0.0.0"
            ping_size = "-1"
            t = "-1"
            ttl = "-1"
            min_t = "-1"
            avg_t = "-1"
            max_t = "-1"
            stddev_t = "-1"

        return {"timestamp": timestamp, "host" : host, "host_ip": host_ip, "ping_size": ping_size, "time": t, "ttl": ttl, "min": min_t, "avg": avg_t, "max": max_t, "stddev": stddev_t}
    
    elif platform.system() == "Windows": # Windows
        p = subprocess.run(["ping", "-n", "1", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        result = p.stdout.split("\n")[2].split(" ")
        host_ip = result[0][:-2]
        ping_size = result[2].split("=")[-1]
        t = result[3].split("=")[-1].split("ms")[0]
        ttl = result[4].split("=")[-1]
        return {"timestamp": timestamp, "host" : host, "host_ip": host_ip, "ping_size": ping_size, "time": t, "ttl": ttl}

    elif platform.system() == "Linux":
        return {}
        
    return {}


def run_ping(host, buffer, duration=10, time_interval=1):
    for tmp in tqdm(range(duration), desc=host):
        result = ping(host)
        buffer.append(result)
        time.sleep(time_interval)


def save_buffer_to_json(buffer, filename):
    with open(filename, "w") as f:
        json.dump(buffer, f, indent=2)

def json_to_df(filename):
    # df = pandas.DataFrame.from_dict(data=buffer, orient="index", columns=["host", "host_ip", "ping_size", "time", "ttl  "])
    df = pandas.read_json(filename)
    return df

def save_df_to_csv(df, filename):
    df.to_csv(filename)

def main(duration):
    hosts = {
        "bithumb": "www.bithumb.com",
        "upbit": "www.upbit.com",
        "binance": "api.binance.com",
        "bitflyer": "api.bitflyer.com",
        "bithumb_api": "api.bithumb.com",
        "upbit_api": "api.upbit.com"
    }

    now = current_timestamp()

    buffer = []
    threads = []

    time_interval = 1

    for key, val in hosts.items():
        th = Thread(target=run_ping, args=(val, buffer, duration, time_interval))
        th.start()
        threads.append(th)

    for th in threads:
        th.join()

    json_filename = now + "_ping_result.json"
    csv_filename = now + "_ping_result.csv"

    print("Save to json file...")
    save_buffer_to_json(buffer, json_filename)
    df = json_to_df(json_filename)
    print("Convert json file into csv file...")
    save_df_to_csv(df, csv_filename)

if __name__ == "__main__":
    args = sys.argv
    if len(args) == 2:
        duration = int(args[1])
        main(duration)
    else:
        print("Usage: python ping_test.py [duration]")
        print("Example: python ping_test.py 10")
        sys.exit(1)