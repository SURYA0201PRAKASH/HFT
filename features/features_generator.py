import pandas as pd
import numpy as np
from pathlib import Path

FILE_PATH = Path("/mnt/c/Group_project/Trading_Platform/hft-prototype/data/features_live.parquet")

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['ema_12'] = df['mid_price'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['mid_price'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']

    delta = df['mid_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / (loss + 1e-9)
    df['rsi_14'] = 100 - (100 / (1 + rs))

    df['vwap'] = (df['trade_price'] * df['trade_qty']).cumsum() / df['trade_qty'].cumsum()
    return df

if __name__ == "__main__":
    df = pd.read_parquet(FILE_PATH)
    print("Before:", df.columns.tolist())
    df = add_indicators(df)
    print("After adding indicators:", df.columns.tolist())
    df.to_parquet(FILE_PATH, index=False)
    print(f"✅ Week 3 indicators added to {FILE_PATH} | rows={len(df)}")


