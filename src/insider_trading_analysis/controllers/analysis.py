import pandas as pd

def total_sec_acq_dis_day(df):
    ''' calculates the total number of securities acquired and disposed per day '''
    acquired_all = df[df["acquiredDisposed"]=="A"].groupby(['periodOfReport'])['totalValue'].sum()
    disposed_all = df[df["acquiredDisposed"]=="D"].groupby(['periodOfReport'])['totalValue'].sum()

    acquired_disposed_all = pd.merge(acquired_all, disposed_all, on='periodOfReport', how='outer')
    acquired_disposed_all.rename(columns={'totalValue_x': 'acquired', 'totalValue_y': 'disposed'}, inplace=True)
    acquired_disposed_all = acquired_disposed_all.sort_values(by=["periodOfReport"])
    return acquired_disposed_all


def companies_bs_in_period(df, year):
    ''' Companies most often bought/sold in a period '''
    periodDf = df['periodOfReport'].dt.year == int(year)
    acquired_by_ticker = df[(df["acquiredDisposed"]=="A") & periodDf] \
        .groupby(["issuerTicker"])['totalValue'].sum().sort_values(ascending=False)

    disposed_by_ticker = df[(df["acquiredDisposed"]=="D") & periodDf] \
        .groupby(["issuerTicker"])['totalValue'].sum().sort_values(ascending=False)

    return (acquired_by_ticker, disposed_by_ticker) 


def companies_bs_in_period_by_person(df, year):
    periodDf = df['periodOfReport'].dt.year == int(year)
    acquired_by_insider = df[(df["acquiredDisposed"]=="A") & periodDf] \
        .groupby(["reporter", "issuerTicker"])['totalValue'] \
        .sum() \
        .sort_values(ascending=False)

    disposed_by_insider = df[(df["acquiredDisposed"]=="D") & periodDf] \
        .groupby(["reporter", "issuerTicker"])['totalValue'] \
        .sum() \
        .sort_values(ascending=False)

    return (acquired_by_insider, disposed_by_insider) 