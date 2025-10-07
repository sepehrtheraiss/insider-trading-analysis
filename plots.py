import os
import pandas as pd
import matplotlib.pyplot as plt

def bar_by_code(df: pd.DataFrame, out_path: str) -> str:
    if df.empty:
        return out_path
    plt.figure()
    (df.groupby(["code"], dropna=False)["dollarValue"].sum()
      .sort_values(ascending=False)
      .plot(kind="bar", rot=0))
    plt.title("Total Insider Dollar Value by Code")
    plt.ylabel("USD")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path

def heatmap_sector_year(pivot: pd.DataFrame, out_path: str) -> str:
    if pivot.empty:
        return out_path
    plt.figure()
    plt.imshow(pivot.fillna(0).values, aspect="auto")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45, ha="right")
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.title("Insider Dollar Value by Sector × Year")
    plt.colorbar()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path
