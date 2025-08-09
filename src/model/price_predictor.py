from datetime import datetime
from pathlib import Path

import lightgbm as lgb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from haversine import haversine
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import KFold

from src.db.db_handler import DBHandler
from src.db.schema import Listing


def get_listing_data() -> pd.DataFrame:
    """Function to retrieve scraped data as a Pandas dataframe.

    Returns:
        pd.DataFrame: Dataframe of scraped listing data.
    """

    handler = DBHandler()
    return handler.read_table(Listing, as_df=True)


def get_postal_df() -> pd.DataFrame:
    """Function to read and process the CSV export of Amsterdam's
    `PC6_PUNTEN_MRA` postal code dataset from
    https://maps.amsterdam.nl/open_geodata/?LANG=en.

    Returns:
        pd.DataFrame: Processed dataframe containing the lat/long coordinates
            per 6-digit postal code.
    """

    f_path = Path(".") / "data" / "files" / "PC6_PUNTEN_MRA.csv"

    df = pd.read_csv(f_path, delimiter=";")
    cols = ["Postcode6", "LNG", "LAT"]

    df = df[cols]
    df.rename(
        columns={"Postcode6": "postcode", "LNG": "long", "LAT": "lat"},
        inplace=True,
    )

    return df[["postcode", "lat", "long"]]


def engineer_features(
    listing: pd.DataFrame, postal: pd.DataFrame, for_pred: bool = False
) -> pd.DataFrame:
    """Function to run feature engineering on a dataframe of property listings.

    Args:
        listing (pd.DataFrame): Dataframe of Amsterdam property listings.
        postal (pd.DataFrame): Dataframe of lat/long coordinates per postal
            code.
        for_pred (bool, optional): Flag to indicate if the feature engineering
            is for an unseen property with no asking price. Defaults to False.

    Returns:
        pd.DataFrame: DataFrame after feature engineering.
    """

    def map_postal_coords(
        df: pd.DataFrame, postal: pd.DataFrame
    ) -> pd.DataFrame:
        """Helper to map lat/long coordinates per postcode.

        Args:
            df (pd.DataFrame): DataFrame of property listings.
            postal (pd.DataFrame): DataFrame containing lat/long coordinates
                per postcode.

        Returns:
            pd.DataFrame: DataFrame with lat/long coordinates mapped based on
                the property's 6-digit postal code.
        """

        # Remove whitespace from listing postal codes
        df["postcode"] = df["postcode"].str.replace(" ", "")
        # Map lat/long coordinates
        df = df.merge(postal, on="postcode", how="inner")
        df.drop("postcode", axis=1, inplace=True)

        return df

    def calc_dst_to_centraal(df: pd.DataFrame) -> pd.DataFrame:
        """Helper function to add the Haversine distance from the listing's
        postal code to Amsterdam Centraal station as a feature.

        Args:
            df (pd.DataFrame): DataFrame after feature engineering.

        Returns:
            pd.DataFrame: DataFrame with distance feature added.
        """

        # Lat/long coordinates of Amsterdam Centraal station
        centraal = (52.3791, 4.8994)
        # Calculate distance to Centraal station based on postal code
        df["dst_to_centraal_km"] = df.apply(
            lambda row: haversine((row["lat"], row["long"]), centraal), axis=1
        )

        return df

    # Columns to use for model training
    if for_pred:
        use_cols = [
            "postcode",
            "size_sqm",
            "room_count",
            "year",
        ]
    else:
        use_cols = [
            "postcode",
            "asking_price_eur",
            "size_sqm",
            "room_count",
            "year",
        ]
    # Filter columns with only useful columns
    df = listing[use_cols].copy()
    # Drop properties without prices
    if not for_pred:
        df = df[df["asking_price_eur"] > 0]
    # Create age from year
    curr_year = datetime.now().year
    df["age"] = curr_year - df["year"]
    df.drop(columns=["year"], inplace=True)
    # Map lat/long coordinates to postal codes
    df = map_postal_coords(df, postal)
    # Create distance to Centraal feature
    df = calc_dst_to_centraal(df)

    return df


def train_predict(df: pd.DataFrame, show_plot: bool = False) -> None:
    """Train a LightGBM model based on scraped property listing data.

    Args:
        df (pd.DataFrame): Dataframe of feature-engineered property listing
            data.
    """

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
    y_log = np.log1p(y)

    # Use KFolds to split data
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    mae_scores, rmse_scores, importances = [], [], np.zeros(len(features))
    for fold, (train_idx, val_idx) in enumerate(kf.split(X)):
        print(f"Fold {fold + 1}:")

        # Train and validate model
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train_log, y_val_log = y_log.iloc[train_idx], y_log.iloc[val_idx]
        y_val = y.iloc[val_idx]

        # Fit LightGBM regressor model trained on the fold
        model = lgb.LGBMRegressor(
            objective="regression",
            n_estimators=1000,
            learning_rate=0.05,
            max_depth=6,
            num_leaves=31,
            lambda_l1=0.1,
            lambda_l2=0.1,
            random_state=fold,
            verbosity=-1,
        )
        model.fit(
            X_train,
            y_train_log,
            eval_set=[(X_val, y_val_log)],
            eval_metric="mae",
            callbacks=[
                lgb.early_stopping(50, verbose=False),
                lgb.log_evaluation(0),
            ],
        )

        # Predict and evaluate
        y_pred_log = model.predict(X_val)
        y_pred_log = np.clip(y_pred_log, a_min=None, a_max=20)
        y_pred = np.expm1(y_pred_log)

        # Calculate and save evaluation metrics
        mae = mean_absolute_error(y_val, y_pred)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mae_scores.append(mae)
        rmse_scores.append(rmse)

        # Save feature importances
        importances += model.feature_importances_

        print(f"\tMAE: {mae:.2f}, RMSE: {rmse:.2f}")

    print(f"\nAverage MAE: {np.mean(mae_scores):.2f}")
    print(f"Average RMSE: {np.mean(rmse_scores):.2f}")

    def plot_importance(importances: np.array, kf: KFold) -> None:
        """Helper function to visually plot the average importance per feature.

        Args:
            importances (np.array): Array of feature importances.
            kf (KFold): KFolds used for model training.
        """

        avg_importance = importances / kf.get_n_splits()
        importance_df = pd.DataFrame(
            {"feature": features, "importance": avg_importance}
        ).sort_values(by="importance", ascending=False)

        plt.figure(figsize=(8, 6))
        plt.barh(importance_df["feature"], importance_df["importance"])
        plt.gca().invert_yaxis()
        plt.xlabel("Average Feature Importance")
        plt.title("LightGBM Feature Importance Across Folds")
        plt.tight_layout()
        plt.show()

    if show_plot:
        plot_importance(importances, kf)

    # Train model on entire dataset
    final_model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=1000,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        lambda_l1=0.1,
        lambda_l2=0.1,
        random_state=42,
        verbosity=-1,
    )
    final_model.fit(X, y_log)

    return final_model, np.mean(mae_scores)


def predict_asking_price(
    model: lgb.LGBMRegressor,
    features: dict,
    postal: pd.DataFrame,
    avg_mae: float,
    show_plot: bool = False,
) -> float:
    """Function to predict an unseen property's asking price using the trained
    LightGBM model.

    Args:
        model (lgb.LGBMRegressor): Trained LightGBM regression model.
        features (dict): Features of the unseen property.
        postal (pd.DataFrame): DataFrame of lat/long coordinates per postal
            code.
        avg_mae (float): Average Mean Absolute Error for the model.

    Returns:
        float: Predicted asking price in Euros.
    """

    # Create dataframe of the listing to predict asking price for
    df = pd.DataFrame([features])
    # Engineer features for the listing
    df = engineer_features(df, postal, for_pred=True)
    # Predict price using model
    log_pred = model.predict(df)
    pred_price = np.expm1(log_pred[0])

    # Get prediction boundary
    lower = pred_price - avg_mae
    upper = pred_price + avg_mae

    # Get actual figure
    actual = features["actual"]

    print(f"Actual asking price: €{actual:,.2f}")
    print(f"Predicted asking price: €{pred_price:,.2f}")
    print(f"Deviation: €{actual - pred_price:,.2f}")
    print(f"Expected range (±MAE): €{lower:,.0f} – €{upper:,.0f}")

    def plot_prediction(
        address: str, pred: float, actual: int, mae: float
    ) -> None:
        """Helper function to plot predicted asking price vs. actual asking
        price.

        Args:
            address (str): Address of the property.
            pred (float): Model's predicted asking price.
            actual (int): Actual asking price.
            mae (float): Average Mean Absolute Error of the model.
        """

        fig, ax = plt.subplots(figsize=(6, 8))
        # Draw error bar of prediction and MAE
        ax.errorbar(
            x=[0],
            y=[pred_price],
            yerr=[[avg_mae], [avg_mae]],
            fmt="o",
            color="lightblue",
            capsize=10,
            label="Predicted ± MAE",
        )
        # Plot actual asking price
        ax.scatter(0, actual, color="red", label="Actual Price", zorder=10)

        # Draw line connecting prediction to actual
        ax.plot([0, 0], [pred_price, actual], linestyle="--", color="gray")

        ax.set_xticks([0])
        ax.set_xticklabels([address])
        ax.set_ylabel("Price (€)")
        ax.set_title(f"Predicted vs Actual: {address}")
        ax.legend()
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        plt.tight_layout()
        plt.show()

    if show_plot:
        plot_prediction(features["address"], pred_price, actual, avg_mae)

    return pred_price


def main() -> None:
    """Main high-level entry point to execute all functions"""

    # Get dataframes of listing and postcode data
    listing_df = get_listing_data()
    postal_df = get_postal_df()
    # Do feature engineering
    df = engineer_features(listing_df, postal_df)
    # Train and return model
    model, avg_mae = train_predict(df, show_plot=False)

    # Features of properties to predict prices for
    properties = [
        {
            "address": "Praterlaan 64",
            "actual": 595000,
            "size_sqm": 96,
            "room_count": 4,
            "year": 2003,
            "postcode": "1098 WR",
        },
        {
            "actual": 280000,
            "address": "Samosstraat 58",
            "size_sqm": 31,
            "room_count": 1,
            "year": 2003,
            "postcode": "1060 TA",
        },
        {
            "actual": 795000,
            "address": "Le Tourmalet 24",
            "size_sqm": 136,
            "room_count": 5,
            "year": 1995,
            "postcode": "1060 NX",
        },
    ]
    for prop in properties:
        predict_asking_price(model, prop, postal_df, avg_mae, show_plot=True)


if __name__ == "__main__":
    main()
