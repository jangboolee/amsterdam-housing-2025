from datetime import datetime
from pathlib import Path

import pandas as pd


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    # Columns to use for model training
    use_cols = [
        "postcode",
        "asking_price_eur",
        "size_sqm",
        "room_count",
        "year",
    ]
    # Filter columns with only useful columns
    df = df[use_cols].copy()
    # Create age from year
    curr_year = datetime.now().year
    df["age"] = curr_year - df["year"]
    df.drop(columns=["year"], inplace=True)


if __name__ == "__main__":
    f_path = Path(".") / "data" / "listing.csv"
    df = pd.read_csv(f_path)
    df = engineer_features(df)
    pass
