import datetime
import subprocess
import time
from threading import Thread
import json
import pandas

def current_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ping(host):
    p = subprocess.run(["ping", "-n", "1", host], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    timestamp = current_timestamp()
    result = p.stdout.split("\n")[2].split(" ")
    host_ip = result[0][:-2]
    ping_size = result[2].split("=")[-1]
    t = result[3].split("=")[-1].split("ms")[0]
    ttl = result[4].split("=")[-1]

    return {"timestamp": timestamp, "host" : host, "host_ip": host_ip, "ping_size": ping_size, "time": t, "ttl": ttl}

def run_ping(host, buffer, time_interval=1):
    duration = 3
    while duration > 0:
        result = ping(host)
        buffer.append(result)
        time.sleep(time_interval)
        duration -= 1

def save_buffer_to_json(buffer, filename):
    with open(filename, "w") as f:
        json.dump(buffer, f, indent=2)

def json_to_df(filename):
    # df = pandas.DataFrame.from_dict(data=buffer, orient="index", columns=["host", "host_ip", "ping_size", "time", "ttl  "])
    df = pandas.read_json(filename)
    return df

def save_df_to_csv(df, filename):
    df.to_csv(filename)

def main():
    hosts = {
        "bithumb": "www.bithumb.com",
        "upbit": "www.upbit.com",
        "binance": "api.binance.com",
        "bitflyer": "api.bitflyer.com",
        "bithumb_api": "api.bithumb.com",
        "upbit_api": "api.upbit.com"
    }

    buffer = []
    threads = []

    for key, val in hosts.items():
        th = Thread(target=run_ping, args=(val, buffer))
        th.start()
        threads.append(th)

    for th in threads:
        th.join()

    json_filename = "ping_result.json"
    csv_filename = "ping_result.csv"
    save_buffer_to_json(buffer, json_filename)
    df = json_to_df(json_filename)
    save_df_to_csv(df, csv_filename)

if __name__ == "__main__":
    main()