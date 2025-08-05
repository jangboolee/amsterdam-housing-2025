from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from haversine import haversine
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import KFold
from sqlalchemy.orm import DeclarativeBase

from src.db.db_handler import DBHandler
from src.db.schema import Listing


def get_listing_data(orm: DeclarativeBase) -> pd.DataFrame:
    """Function to retrieve scraped data as a Pandas dataframe.

    Args:
        orm (DeclarativeBase): ORM of scraped listing data.

    Returns:
        pd.DataFrame: Dataframe of scraped listing data.
    """

    handler = DBHandler()
    return handler.read_table(Listing, as_df=True)


def get_postal_df(f_path: Path) -> pd.DataFrame:
    df = pd.read_csv(f_path, delimiter=";")
    cols = ["Postcode6", "LNG", "LAT"]

    df = df[cols]
    df.rename(
        columns={"Postcode6": "postcode", "LNG": "long", "LAT": "lat"},
        inplace=True,
    )

    return df[["postcode", "lat", "long"]]


def engineer_features(
    listing: pd.DataFrame, postal: pd.DataFrame
) -> pd.DataFrame:
    def map_postal_coords(df: pd.DataFrame, postal: pd.DataFrame):
        # Remove whitespace from listing postal codes
        df["postcode"] = df["postcode"].str.replace(" ", "")
        # Map lat/long coordinates
        df = df.merge(postal, on="postcode", how="inner")
        df.drop("postcode", axis=1, inplace=True)
        return df

    def calc_dst_to_centraal(df: pd.DataFrame):
        # Lat/long coordinates of Amsterdam Centraal station
        centraal = (52.3791, 4.8994)
        # Calculate distance to Centraal station based on postal code
        df["dst_to_centraal_km"] = df.apply(
            lambda row: haversine((row["lat"], row["long"]), centraal), axis=1
        )

        return df

    # Columns to use for model training
    use_cols = [
        "postcode",
        "asking_price_eur",
        "size_sqm",
        "room_count",
        "year",
    ]
    # Filter columns with only useful columns
    df = listing[use_cols].copy()
    # Create age from year
    curr_year = datetime.now().year
    df["age"] = curr_year - df["year"]
    df.drop(columns=["year"], inplace=True)
    # Map lat/long coordinates to postal codes
    df = map_postal_coords(df, postal)
    # Create distance to Centraal feature
    df = calc_dst_to_centraal(df)

    return df


def train_predict(df: pd.DataFrame):
    # Define features and target variable
    features = [
        "size_sqm",
        "room_count",
        "age",
        "lat",
        "long",
        "dst_to_centraal_km",
    ]
    target = "asking_price_eur"

    # Separate target variable from model features with log transformation
    X = df[features]
    y = df[target]

    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    mae_scores, rmse_scores = [], []
    for fold, (train_idx, val_idx) in enumerate(kf.split(X)):
        print(f"Fold {fold + 1}:")

        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        # Train LigtGBM regressor model
        model = lgb.LGBMRegressor(
            objective="regression",
            n_estimators=1000,
            learning_rate=0.05,
            max_depth=6,
            num_leaves=31,
            lambda_l1=0.1,
            lambda_l2=0.1,
            random_state=fold,
        )
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            eval_metric="mae",
        )

        # Predict and evaluate
        y_pred = model.predict(X_val)

        mae = mean_absolute_error(y_val, y_pred)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))

        mae_scores.append(mae)
        rmse_scores.append(rmse)

        print(f"  MAE: {mae:.2f}, RMSE: {rmse:.2f}")

    print(f"\nAverage MAE: {np.mean(mae_scores):.2f}")
    print(f"Average RMSE: {np.mean(rmse_scores):.2f}")


if __name__ == "__main__":
    # Get dataframes of listing and postcode data
    listing_df = get_listing_data(Listing)
    postal_df = get_postal_df(Path(".") / "data" / "PC6_PUNTEN_MRA.csv")
    # Do feature engineering
    df = engineer_features(listing_df, postal_df)
    train_predict(df)
