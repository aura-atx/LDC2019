from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


replacements = {
    "(3) Services (cont.)": "(3) Services",
    "(7) Restaurant and Bars": "(7) Restaurant and Bar",
    "(8) Entertainment and Recreation (cont.)": "(8) Entertainment and Recreation",
    "(8) Entertainment and": "(8) Entertainment and Recreation",
    ">100,000 sf": "> 100,000 sf",
    "Alcohol Sales 23-3D-1070": "Alcohol Sales",
    "Automobile Repair —": "Automobile Repair",
    "Bar/ Nightclub": "Bar/Nightclub",
    "Business, or Trade": "Business or Trade",
    "Business and Financial/Professional": "Business and Financial/Professional Services",
    "Cohousing": "Co-housing",
    "Drive -Through": "Drive-Through",
    "Drive Through, Retail, or Service Facility": "Drive-Through, Retail, or Service Facility",
    "Drop-off and Reuse": "Drop-off and Reuse Facility",
    "Funeral/ Mortuary Home": "Funeral/Mortuary Home",
    "Hazardous": "Hazardous Materials",
    "Library, Museum, or Public Art": "Library, Museum, or Public Art Gallery",
    "Library, Museum, or Public": "Library, Museum, or Public Art Gallery",
    "Meeting Facility (public or": "Meeting Facility (public or private)",
    "Micro-Brewery/Micro-Distillery/": "Micro-Brewery/Micro-Distillery/Winery",
    "Mobile Food Sales 23-3D-1240": "Mobile Food Sales",
    "Mobile Retail Sales 23-3D-1260": "Mobile Retail Sales",
    "Mobile Retail": "Mobile Retail Sales",
    "Multi-family": "Multi-Family",
    "Non-Hazardous": "Non-Hazardous Materials",
    "Outdoor; Late-Night": "Outdoor; Late Night",
    "Pawn Shop": "Pawn Shops",
    "RESIDENTIAL": "Residential",
    "Recreational and Sports Vehicle Sales,": "Recreational and Sports Vehicle Sales, Rental, and Storage",
    "Type 1": "Types 1",
    "Type 3": "Types 3",
    "w/ Alcohol Sales 23-3D-1320": "w/ Alcohol Sales",
    "w/ Alcohol Sales,23-3D-1070": "w/ Alcohol Sales",
    "w/ Outside Storage 23-3D-1200": "w/ Outside Storage",
    "≤ 100,000 sq ft": "≤ 100,000 sf",
    "≤5,000 sf": "≤ 5,000 sf",
}


def normalize(df):
    """Normalize the dataframe."""
    df["Use Type"] = df["Use Type"].str.strip()
    df["Specific to Use Requirements"] = df["Specific to Use Requirements"].str.strip()
    df.replace({"Use Type": replacements}, inplace=True)


def merge(left, right):
    """Merge 2 csv files."""
    normalize(right)
    return pd.merge(left, right, on=["Use Type", "Specific to Use Requirements"], how="outer")


def main():
    """."""
    p = Path(".")
    csvs = list(p.glob("**/csv/*.csv"))
    joined = pd.read_csv(str(csvs[0]))
    normalize(joined)
    logger.debug(f"merging with {csvs[0]}")
    for csv in csvs[1:]:
        logger.debug(f"merging with {csv}")
        right = pd.read_csv(str(csv))
        joined = merge(joined, right)

    # Clean up the final dataframe
    # We should not need to remove the duplicates.
    # joined.drop_duplicates(["Use Type"], inplace=True)

    # Remove the empty rows.
    # joined.replace("", np.nan, inplace=True)
    # joined.dropna(axis=0, how="all", inplace=True)

    # Re-index and sort the df.
    joined.set_index("Use Type", inplace=True)
    joined.sort_values("Use Type", inplace=True)

    # Export the final dataframe.
    joined.to_csv("merge.csv")

    # Print stats.
    print(f"Shape: {joined.shape}")


if __name__ == "__main__":
    main()
