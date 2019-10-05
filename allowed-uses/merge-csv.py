from pathlib import Path

import pandas as pd
from loguru import logger


def normalize(df):
    """Normalize the dataframe."""
    df["Use Type"] = df["Use Type"].str.strip()
    df["Specific to Use Requirements"] = df[
        "Specific to Use Requirements"].str.strip()


def merge(left, right):
    """Merge 2 csv files."""
    normalize(left)
    normalize(right)
    return pd.merge(left,
                    right,
                    on=["Use Type", "Specific to Use Requirements"],
                    how="outer")


def main():
    """."""
    p = Path(".")
    csvs = list(p.glob("**/csv/*.csv"))
    joined = pd.read_csv(str(csvs[0]))
    logger.debug(f"merging with {csvs[0]}")
    for csv in csvs[1:]:
        logger.debug(f"merging with {csv}")
        right = pd.read_csv(str(csv))
        joined = merge(joined, right)

    # Clean up the final dataframe
    joined.drop_duplicates(["Use Type"], inplace=True)
    joined.set_index("Use Type", inplace=True)
    joined.sort_values("Use Type", inplace=True)

    # Export the final dataframe.
    joined.to_csv("merge.csv")


if __name__ == "__main__":
    main()
