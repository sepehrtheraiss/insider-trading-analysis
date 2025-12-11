import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from .present import millions_formatter

plt.style.use('seaborn-v0_8')
#plt.style.use('seaborn-v0_8-darkgrid')
#plt.style.use('seaborn-v0_8-whitegrid')
#plt.style.use('seaborn-v0_8-dark')
#plt.style.use('seaborn-v0_8-talk')

params = {'legend.fontsize': '14',
          'font.size': '14',
          'axes.labelsize': '14',
          'axes.labelweight': 'bold',
          'axes.titlesize':'14',
          'xtick.labelsize':'14',
          'ytick.labelsize':'14'
         }

plt.rcParams.update(params)

# =====================================================================
# 1. Acquired / Disposed Per Year
# =====================================================================

def plot_amount_assets_acquired_disposed(
    df,
    *,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    """
    df columns: ['acquired', 'disposed']
    index: DatetimeIndex grouped by year
    """

    ax = df.plot.bar(
        figsize=(15, 7),
        stacked=False,
        color=["#2196f3", "#ef5350"]
    )

    ax.grid(True)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    ax.set_xlabel("Year")
    ax.set_ylabel("Amount $")
    ax.set_title("Acquired / Disposed per Year")
    ax.set_xticks(range(len(df.index)))
    ax.set_xticklabels([ts.strftime("%Y") for ts in df.index])
    ax.figure.autofmt_xdate(rotation=0, ha='center')

    plt.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/amount_assets_{start}_{end}.png", dpi=150)
    if show:
        plt.show()



# =====================================================================
# 2. Distribution of Transaction Codes
# =====================================================================

def plot_distribution_trans_codes(
    series,
    *,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    """
    series index: MultiIndex (acquired_disposed, code)
    series values: total_value
    """

    ax = series.plot.barh(figsize=(20, 10))
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))

    ax.set_xlabel("Amount $")
    ax.set_ylabel("Transaction Code")
    ax.set_title(f"Distribution of Transaction Codes ({start} → {end})")

    plt.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/distribution_codes_{start}_{end}.png", dpi=150)
    if show:
        plt.show()



# =====================================================================
# 3. Top N Companies Bought / Sold
# =====================================================================

def plot_n_most_companies_bs(
    acquired,
    disposed,
    *,
    n,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    """
    acquired: Series indexed by issuer_ticker
    disposed: same structure
    """

    fig, axes = plt.subplots(1, 2, figsize=(20, 10))

    # LEFT — ACQUIRED
    axA = acquired.head(n).sort_values().plot.barh(
        ax=axes[0], color="#2196f3"
    )
    axA.xaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    axA.set_xlabel("Amount $Millions")
    axA.set_ylabel("Ticker")
    axA.set_title(f"Top {n} Most Bought Companies in {start}-{end}")

    # RIGHT — DISPOSED
    axD = disposed.head(n).sort_values().plot.barh(
        ax=axes[1], color="#ef5350"
    )
    axD.xaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    axD.set_xlabel("Amount $Millions")
    axD.set_ylabel("Ticker")
    axD.set_title(f"Top {n} Most Sold Companies in {start}-{end}")

    fig.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/top_{n}_companies_{start}_{end}.png", dpi=150)
    if show:
        plt.show()



# =====================================================================
# 4. Top N Companies Bought / Sold by reporter
# =====================================================================

def plot_n_most_companies_bs_by_reporter(
    acquired,
    disposed,
    *,
    n,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    """
    acquired index: (reporter, issuer_ticker)
    disposed index: (reporter, issuer_ticker) 
    """

    # Format long names for rendering
    def _fmt(idx):
        name, ticker = idx
        return (name[:30] + "..") if len(name) > 30 else name, ticker

    acquired.index = acquired.index.map(_fmt)
    disposed.index = disposed.index.map(_fmt)

    fig, axes = plt.subplots(1, 2, figsize=(20, 10))

    # LEFT — BUYERS
    axA = acquired.head(n).sort_values().plot.barh(
        ax=axes[0], color="#2196f3"
    )
    axA.xaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    axA.set_xlabel("Amount $Millions")
    axA.set_ylabel("Insider, Ticker")
    axA.set_title(f"Top {n} Buyers in {start}-{end}")

    # RIGHT — SELLERS
    axD = disposed.head(n).sort_values().plot.barh(
        ax=axes[1], color="#ef5350"
    )
    axD.xaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    axD.set_xlabel("Amount $Millions")
    axD.set_ylabel("Insider, Ticker")
    axD.set_title(f"Top {n} Sellers in {start}-{end}")

    fig.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/top_{n}_by_reporter_{start}_{end}.png", dpi=150)
    if show:
        plt.show()



# =====================================================================
# 5. Price Line Chart
# =====================================================================

def plot_line_chart(
    df,
    *,
    ticker,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    """
    df is OHLC dataframe with a 'Close' column.
    """

    ax = df["Close"].plot(
        figsize=(15, 7),
        title=f"{ticker} Daily Price Chart",
        xlabel="Date",
        ylabel="Price $"
    )

    plt.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/{ticker}_line_chart_{start}_{end}.png", dpi=150)
    if show:
        plt.show()



# =====================================================================
# 6. Sector Statistics by Year
# =====================================================================

def plot_sector_stats(
    df,
    *,
    save=False,
    outpath=None,
    show=False,
    start=None,
    end=None
):
    fig, ax = plt.subplots(figsize=(17, 10))

    unstacked = df.unstack()
    unstacked.plot.bar(stacked=True, ax=ax)

    ax.yaxis.set_major_formatter(mtick.FuncFormatter(millions_formatter))
    ax.set_xlabel("Year")
    ax.set_ylabel("Amount $")
    ax.set_title("Sector Statistics")

    years = [idx[0].year for idx in unstacked.index]
    ax.set_xticks(range(len(years)))
    ax.set_xticklabels([str(y) for y in years])


    ax.grid(True)
    plt.tight_layout()

    if save and outpath:
        plt.savefig(f"{outpath}/sector_stats_{start}_{end}.png", dpi=150)
    if show:
        plt.show()