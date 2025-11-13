import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from utils.utils import millions_formatter

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

def plot_annual_graph(df, args):
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

    plt.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/annual_graph_{args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

# Distribution of Transaction Codes
def plot_distribution_trans_codes(df, args):
    year_start = df.head(1)['periodOfReport'].dt.year.item()
    year_end = df.tail(1)['periodOfReport'].dt.year.item()
    transaction_code = df.groupby(["acquiredDisposed", "code"])['totalValue'].sum() 
    ax_codes = transaction_code.plot.barh(figsize=(10,8))
    ax_codes.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_codes.set_xlabel("Amount $")
    ax_codes.set_ylabel("Transaction Code")
    ax_codes.set_title(f"Distribution of Transaction Codes ({year_start} - {year_end})")
    ax_codes.figure.autofmt_xdate(rotation=0, ha='center')

    plt.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/annual_graph_{args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

# N companies bought/sold in a period
def plot_n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, args):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 5))
    ax_ac_ti = acquired_by_ticker.head(args.n).sort_values(ascending=True).plot.barh(ax=axes[0], y='issuerTicker')
    ax_ac_ti.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_ac_ti.set_xlabel("Amount $")
    ax_ac_ti.set_ylabel("Ticker")
    ax_ac_ti.set_title(f"Top {args.n} Most Bought Companies in {args.year}")
    ax_ac_ti.figure.autofmt_xdate(rotation=0, ha='center')

    ax_di_ti = disposed_by_ticker.head(args.n).sort_values(ascending=True).plot.barh(ax=axes[1], y='issuerTicker')
    ax_di_ti.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_di_ti.set_xlabel("Amount $")
    ax_di_ti.set_ylabel("Ticker")
    ax_di_ti.set_title(f"Top {args.n} Most Sold Companies in {args.year}")
    ax_di_ti.figure.autofmt_xdate(rotation=0, ha='center')

    fig.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/annual_graph_{args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()