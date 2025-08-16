from pathlib import Path

import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from src.model.price_predictor import get_listing_data


def get_data() -> pd.DataFrame:
    """Function to retrieve a dataframe of listing data.

    Returns:
        pd.DataFrame: A DataFrame of listings, scraped from pararius.nl.
    """
    return get_listing_data()


def process_listing_data(df: pd.DataFrame) -> pd.DataFrame:
    """Function to process listing data, with the following steps:
        1. Drop columns unnecessary for visualization
        2. Drop properties without prices
        3. Create price/sqm feature
        4. Standardize postcode
        5. Create postcode4
        6. Get median price/sqm per postcode

    Args:
        df (pd.DataFrame): A DataFrame of listings, scraped from pararius.nl.

    Returns:
        pd.DataFrame: DataFrame of median asking price/sqm, grouped by postal
            codes.
    """

    def groupby_median(df: pd.DataFrame, by_col: str) -> pd.DataFrame:
        """Helper function to dynamically groupby based on median price/sqm.

        Args:
            df (pd.DataFrame): Processed listing data.
            by_col (str): Column name to run groupby on.

        Returns:
            pd.DataFrame: DataFrame of median asking price/sqm per postcode.
        """

        # Drop other postcode column based on used postcode column
        if by_col == "postcode4":
            df = df.drop("postcode6", axis=1)
        else:
            df = df.drop("postcode4", axis=1)

        return df.groupby(by_col).median()

    # Filter for only columns used for postcode-level visualization
    use_cols = ["postcode", "asking_price_eur", "size_sqm"]
    df = df[use_cols].copy()
    # Drop properties without prices
    df = df[df["asking_price_eur"] > 0]
    # Create asking price/sqm feature
    df["price_per_sqm"] = df["asking_price_eur"] / df["size_sqm"]
    # Drop size and price column
    to_drop = ["asking_price_eur", "size_sqm"]
    for col in to_drop:
        df.drop(col, axis=1, inplace=True)
    # Remove whitespace and create postcode4
    df["postcode6"] = df["postcode"].str.replace(" ", "")
    df["postcode4"] = df["postcode"].str.slice(stop=4)
    df.drop("postcode", axis=1, inplace=True)
    # Get median asking price/sqm per postalcode
    pc4_median = groupby_median(df, "postcode4")
    pc6_median = groupby_median(df, "postcode6")

    return pc4_median, pc6_median


def plot_postcode(df: pd.DataFrame, geojson_path: Path, column: str) -> bool:
    """Function to plot the median asking price per sqm in both static image
    files and interactive maps.

    Args:
        df (pd.DataFrame): DataFrame of processed listing data.
        geojson_path (Path): Path of the GeoJSON file containing postal code
            polygons.
        column (str): Name of the column to base visualizations on.

    Returns:
        bool: True after completion.
    """

    # Create geodataframe
    gdf = gpd.read_file(geojson_path)
    # Merge by postcode
    gdf_merged = gdf.merge(
        df, left_on=column, right_on=column.lower(), how="left"
    )
    # Create coordinates column
    gdf_merged["coords"] = gdf_merged["geometry"].apply(
        lambda x: x.representative_point().coords[:]
    )
    gdf_merged["coords"] = [coords[0] for coords in gdf_merged["coords"]]
    # Create separate price column for map display
    gdf_merged["price_per_sqm_display"] = gdf_merged["price_per_sqm"].apply(
        lambda x: f"{x:,.2f}"
    )

    def show_save_static_plots(show: bool = False) -> bool:
        """Helper function to display and save static postcode-level
        visualizations.

        Args:
            show (bool, optional): Flag to show the map before saving to a
                file. Defaults to True.

        Returns:
            bool: True after completion.
        """

        # Plot median asking price per postcode
        fig, ax = plt.subplots(figsize=(20, 12))
        gdf_merged.plot(
            column="price_per_sqm",
            cmap="viridis",
            legend=True,
            edgecolor="black",
            linewidth=0.2,
            ax=ax,
            missing_kwds={"color": "lightgrey", "label": "No listings"},
        )

        # Create pricelabel based on coordinates
        if column == "Postcode4":
            for idx, row in gdf_merged.iterrows():
                plt.annotate(
                    text=f"{row['price_per_sqm']:,.0f}",
                    xy=row["coords"],
                    horizontalalignment="center",
                    bbox=dict(facecolor="white", alpha=0.5, edgecolor="none"),
                )

        ax.set_title("Amsterdam median asking price (€) per sqm", fontsize=16)
        ax.axis("off")

        # Dynamically set save path based on used column
        if column == "Postcode6":
            save_path = Path(".") / "output" / "median_per_postcode6.png"
        else:
            save_path = Path(".") / "output" / "median_per_postcode4.png"

        plt.tight_layout()
        plt.savefig(save_path, bbox_inches="tight", dpi=300)
        if show:
            plt.show()

        print(f"Static map for {column} saved.")

        return True

    def save_folium_map() -> bool:
        """Helper function to generate an interactive Folium map of median
        asking price/sqm per postcode.

        Returns:
            bool: True after completion.
        """

        # Get center of map
        center_lng, center_lat = (
            gdf_merged.geometry.union_all().centroid.coords[:][0]
        )
        map_center = (center_lat, center_lng)

        # Create folium map
        m = folium.Map(
            location=map_center, zoom_start=12, tiles="cartodbpositron"
        )

        # Add choropleth layer to folium map
        folium.Choropleth(
            geo_data=gdf_merged[[column, "geometry"]],
            name="Price per sqm",
            data=gdf_merged[[column, "price_per_sqm"]],
            columns=[column, "price_per_sqm"],
            key_on=f"feature.properties.{column}",
            fill_color="YlGnBu",
            fill_opacity=0.4,
            line_opacity=0.2,
            nan_fill_color="lightgray",
            legend_name="Median price per sqm (€)",
        ).add_to(m)
        # Add tooltips to folium map
        tooltip = folium.GeoJsonTooltip(
            fields=[column, "price_per_sqm_display"],
            aliases=["Postcode", "Median price per sqm (€)"],
        )
        folium.GeoJson(
            gdf_merged,
            tooltip=tooltip,
            style_function=lambda x: {
                "fillOpacity": 0,
                "color": "black",
                "weight": 0.1,
            },
        ).add_to(m)

        # Save to HTML
        if column == "Postcode6":
            save_path = Path(".") / "output" / "pc6_interactive_price_map.html"
        else:
            save_path = Path(".") / "output" / "pc4_interactive_price_map.html"
        m.save(save_path)

        print(f"Interactive map for {column} saved.")

        return True

    # Display and save static plots
    show_save_static_plots()
    # Save interactive folium maps
    save_folium_map()


def main() -> None:
    """High-level entry point to execute all functions"""

    # Get and process listing dataframe
    listing_df = get_data()
    pc4_median, pc6_median = process_listing_data(listing_df)

    # Iteratively plot and save visualizations
    to_plot = (
        (
            pc4_median,
            Path(".") / "data" / "files" / "postcode4_lnglat.geojson",
            "Postcode4",
        ),
        (
            pc6_median,
            Path(".") / "data" / "files" / "postcode6_lnglat.geojson",
            "Postcode6",
        ),
    )
    for median, geojson_path, column in to_plot:
        plot_postcode(median, geojson_path, column)


if __name__ == "__main__":
    main()
