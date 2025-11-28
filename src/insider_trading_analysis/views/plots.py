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

def plot_amount_assets_acquired_disposed(df, args):
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
        plt.savefig(f'{args.outpath}/amount of assets acquired and disposed {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

# Distribution of Transaction Codes
def plot_distribution_trans_codes(df, args):
    year_start = df.head(1)['periodOfReport'].dt.year.item()
    year_end = df.tail(1)['periodOfReport'].dt.year.item()
    transaction_code = df.groupby(["acquiredDisposed", "code"])['totalValue'].sum() 
    ax_codes = transaction_code.plot.barh(figsize=(20,10))
    ax_codes.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_codes.set_xlabel("Amount $")
    ax_codes.set_ylabel("Transaction Code")
    ax_codes.set_title(f"Distribution of Transaction Codes ({year_start} - {year_end})")
    ax_codes.figure.autofmt_xdate(rotation=0, ha='center')

    plt.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/Distribution of Transaction Codes {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

# N companies bought/sold in a period
def plot_n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, args):
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 10))
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
        plt.savefig(f'{args.outpath}/Top {args.n} companies bought & sold in {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()


def name_formatter(tup):
  name, ticker = tup
  name = name[:30] + '..' if len(name) > 30 else name
  return (name, ticker)

def plot_n_most_companies_bs_by_person(acquired_by_ticker, disposed_by_ticker, args):
    acquired_by_ticker.index = acquired_by_ticker.index.map(name_formatter)
    disposed_by_ticker.index = disposed_by_ticker.index.map(name_formatter)
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 10))

    ax_ac_in = acquired_by_ticker.head(10).sort_values(ascending=True).plot.barh(ax=axes[0], y='reportingPerson')
    ax_ac_in.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_ac_in.set_xlabel("Amount $")
    ax_ac_in.set_ylabel("Insider, Ticker")
    ax_ac_in.set_title(f"Top {args.n} Largest Buyers in {args.year}")
    ax_ac_in.figure.autofmt_xdate(rotation=0, ha='center')

    ax_di_in = disposed_by_ticker.head(10).sort_values(ascending=True).plot.barh(ax=axes[1], y='reportingPerson')
    ax_di_in.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax_di_in.set_xlabel("Amount $")
    ax_di_in.set_ylabel("Insider, Ticker")
    ax_di_in.set_title(f"Top {args.n} Largest Sellers {args.year}")
    ax_di_in.figure.autofmt_xdate(rotation=0, ha='center')

    fig.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/Top {args.n} companies bought & sold by insider {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

def plot_line_chart(ohcl, args):

    ohcl["Close"].plot(xlabel="Date", ylabel="Price $", title=f"{args.ticker} Daily Price Chart", figsize=(15,7))
         
    plt.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/{args.ticker} Daily Price Chart {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()

def plot_acquired_disposed_line_chart(ticker_series, title="", ylabel="Price $", args=None):
    ax = ticker_series["Close"].plot(figsize=(20, 7))
    ax.xaxis_date()
    ax.plot(ticker_series.index, ticker_series['Close'], lw=2)

    max_acquired = ticker_series['totalValue'].max()
    x = 0
    # draw markers onto price time series. Marker size correlates to news volume.
    for index, row in ticker_series.iterrows():
        if row['totalValue'] == 0:
            continue

        x += 1
        markersize = (row['totalValue'] / max_acquired) * 25

        if markersize < 7:
            markersize = 7

        color = 'green' if row['acquiredDisposed'] == 'A' else 'red'
        if x == 1:
            color = 'green'
        if x == 2:
            color = 'red'
        ax.plot([index], 
                [row['Close']], 
                marker='o', 
                color=color, 
                markersize=markersize)

        # overlay arrow pointer at largest news volume 
        if row['totalValue'] == max_acquired:
            ax.annotate(
            '\n' + "$ {:,.0f}".format(row['totalValue']),
            xy=(index, row['Close']), xycoords='data',
            xytext=(0, -30), textcoords='offset pixels',
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3", color='red')
            )

    ax.set_ylabel(ylabel)
    ax.set_xlabel('Day')
    ax.set_title(title)

    total_value = ticker_series['totalValue'].sum()
    text_y_pos = (ticker_series['Close'].max() + ticker_series['Close'].min()) / 2
    ax.annotate(
    'totalValue amount: ' + "$ {:,.0f}".format(total_value),
    xy=(ticker_series.index[10], text_y_pos), xycoords='data',
    xytext=(-100, -100), textcoords='offset pixels',
    )

    plt.setp(plt.gca().get_xticklabels(), rotation = 0, ha='center')

    plt.tight_layout()
    if args.save:
        plt.savefig(f'{args.outpath}/{args.ticker} Daily Price Chart {args.start}-{args.end}', dpi=150)
    if args.show:
        plt.show()