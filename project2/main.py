import os
from feature_calc import *

if __name__ == "__main__":
    raw_fn = "2024-05-01-upbit-BTC-feature"
    if not os.path.exists(indicators_csv(raw_fn)):
        faster_calc_indicators(raw_fn)
        print("done")

    full_df = pd.read_csv(raw_fn + ".csv")

    for i in range(0, 24, 3):
        start_timestamp = f"2024-05-01 {i:02d}:00:00"
        end_timestamp = f"2024-05-01 {i+2:02d}:59:59"

        df = full_df.copy()
        df = df[
            (df["timestamp"] >= start_timestamp) & (df["timestamp"] <= end_timestamp)
        ]

        filename = indicators_csv(raw_fn, start_timestamp.split(" ")[1].split(":")[0])
        if not os.path.exists(filename):
            df.to_csv(filename, index=False)
            print("done")
        else:
            print("already exists")
