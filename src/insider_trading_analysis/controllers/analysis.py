import pandas as pd

def total_sec_acq_dis_day(df):
    ''' calculates the total number of securities acquired and disposed per day '''
    acquired_all = df[df["acquired_disposed"]=="A"].groupby(['period_of_report'])['total_value'].sum()
    disposed_all = df[df["acquired_disposed"]=="D"].groupby(['period_of_report'])['total_value'].sum()

    acquired_disposed_all = pd.merge(acquired_all, disposed_all, on='period_of_report', how='outer')
    acquired_disposed_all.rename(columns={'totalValue_x': 'acquired', 'totalValue_y': 'disposed'}, inplace=True)
    acquired_disposed_all = acquired_disposed_all.sort_values(by=["period_of_report"])
    return acquired_disposed_all


def companies_bs_in_period(df, year):
    ''' Companies most often bought/sold in a period '''
    periodDf = df['period_of_report'].dt.year == int(year)
    acquired_by_ticker = df[(df["acquired_disposed"]=="A") & periodDf] \
        .groupby(["issuer_ticker"])['total_value'].sum().sort_values(ascending=False)

    disposed_by_ticker = df[(df["acquired_disposed"]=="D") & periodDf] \
        .groupby(["issuer_ticker"])['total_value'].sum().sort_values(ascending=False)

    return (acquired_by_ticker, disposed_by_ticker) 


def companies_bs_in_period_by_person(df, year):
    periodDf = df['period_of_report'].dt.year == int(year)
    acquired_by_insider = df[(df["acquired_disposed"]=="A") & periodDf] \
        .groupby(["reporter", "issuer_ticker"])['total_value'] \
        .sum() \
        .sort_values(ascending=False)

    disposed_by_insider = df[(df["acquired_disposed"]=="D") & periodDf] \
        .groupby(["reporter", "issuer_ticker"])['total_value'] \
        .sum() \
        .sort_values(ascending=False)

    return (acquired_by_insider, disposed_by_insider) 