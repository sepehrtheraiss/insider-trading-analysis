import pandas as pd

def name_formatter(tup):
    name, ticker = tup
    if len(name) > 30:
        name = name[:30] + ".."
    return name, ticker

def sector_year_pivot(df: pd.DataFrame):
    dfc = df.copy()
    dfc["year"] = dfc["transaction_date"].dt.year
    return dfc.pivot_table(
        index="sector",
        columns="year",
        values="total_value",
        aggfunc="sum",
    )

# Prettify y axis: 2000000 to $2M
def millions_formatter(x,y=0):
    #return '$ {:,.0f} M'.format(x*1e-6)
    return '{:,.0f}'.format(x*1e-6)