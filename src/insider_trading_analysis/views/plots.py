import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from utils.utils import millions_formatter


def savefig_bar_by_code(df: pd.DataFrame, out_path: str) -> bool:
    if df.empty:
        return False 
    plt.figure()
    (df.groupby(["code"], dropna=False)["totalValue"].sum()
      .sort_values(ascending=False)
      .plot(kind="bar", rot=0))
    plt.title("Total Insider Total Value by Code")
    plt.ylabel("USD")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return True 

def savefig_heatmap_sector_year(pivot: pd.DataFrame, out_path: str) -> bool:
    if pivot.empty:
        return False 
    plt.figure()
    plt.imshow(pivot.fillna(0).values, aspect="auto")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=45, ha="right")
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.title("Insider Total Value by Sector × Year")
    plt.colorbar()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return True 

def plot_annual_graph(df):
  df.index = pd.to_datetime(df.index)

  acquired_yr = df.groupby(pd.Grouper(freq='Y'))['acquired'].sum()
  disposed_yr = df.groupby(pd.Grouper(freq='Y'))['disposed'].sum()

  acquired_disposed_yr = pd.merge(acquired_yr, disposed_yr, on='periodOfReport', how='outer')

  ax = acquired_disposed_yr.plot.bar(stacked=False, figsize=(15, 7), color=["#2196f3", "#ef5350"])

  ax.grid(True)
  ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
  ax.set_xticks(range(acquired_disposed_yr.index.size))
  ax.set_xticklabels([ts.strftime('%Y') for idx, ts in enumerate(acquired_disposed_yr.index)])
  ax.set_xlabel("Year")
  ax.set_ylabel("Amount $")
  ax.set_title("Acquired/Disposed per Year")
  ax.figure.autofmt_xdate(rotation=0, ha='center')
  
  plt.show()

# Distribution of Transaction Codes
def plot_distribution_trans_codes(df):
    transaction_code = df.groupby(["acquiredDisposed", "code"])['totalValue'].sum() 
    ax_codes = transaction_code.plot.barh(figsize=(10,8))
    ax_codes.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_codes.set_xlabel("Amount $")
    ax_codes.set_ylabel("Transaction Code")
    ax_codes.set_title("Distribution of Transaction Codes (2012 - 2022)")
    ax_codes.figure.autofmt_xdate(rotation=0, ha='center')

    plt.show()

# N companies bought/sold in a period
def plot_n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, year, n=15):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 5))

    ax_ac_ti = acquired_by_ticker.head(n).sort_values(ascending=True).plot.barh(ax=axes[0], y='issuerTicker')
    ax_ac_ti.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_ac_ti.set_xlabel("Amount $")
    ax_ac_ti.set_ylabel("Ticker")
    ax_ac_ti.set_title(f"Top {n} Most Bought Companies in {year}")
    ax_ac_ti.figure.autofmt_xdate(rotation=0, ha='center')

    ax_di_ti = disposed_by_ticker.head(n).sort_values(ascending=True).plot.barh(ax=axes[1], y='issuerTicker')
    ax_di_ti.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_di_ti.set_xlabel("Amount $")
    ax_di_ti.set_ylabel("Ticker")
    ax_di_ti.set_title(f"Top {n} Most Sold Companies in {year}")
    ax_di_ti.figure.autofmt_xdate(rotation=0, ha='center')

    fig.tight_layout()
    plt.show()