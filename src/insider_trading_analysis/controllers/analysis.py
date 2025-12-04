import pandas as pd

def total_sec_acq_dis_day(df):
    """
    Calculates the total dollar value of securities acquired and disposed per day.
    Uses snake_case column names (`acquired_disposed`, `total_value`, `period_of_report`).
    """

    # Group: acquired (A)
    acquired_all = (
        df[df["acquired_disposed"] == "A"]
        .groupby("period_of_report")["total_value"]
        .sum()
        .rename("acquired")
    )

    # Group: disposed (D)
    disposed_all = (
        df[df["acquired_disposed"] == "D"]
        .groupby("period_of_report")["total_value"]
        .sum()
        .rename("disposed")
    )

    # Merge the two series on period_of_report
    out = pd.concat([acquired_all, disposed_all], axis=1)

    # Replace NaN with 0 for missing days
    out = out.fillna(0)

    # Sort by date
    out = out.sort_values(by="period_of_report")

    return out


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