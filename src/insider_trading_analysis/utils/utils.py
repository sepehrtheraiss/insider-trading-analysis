from datetime import datetime, timedelta

# Prettify y axis: 2000000 to $2M
def millions_formatter(x,y=0):
    return '$ {:,.0f} M'.format(x*1e-6)

def iterate_months(start_date: str, end_date: str):
    """Yield monthly date ranges between start_date and end_date (inclusive of start)."""
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    months = []

    while current < end:
        #yield current.strftime("%Y-%m-%d")
        months.append(current.strftime("%Y-%m-%d"))
        # Move to next month
        year = current.year + (current.month // 12)
        month = (current.month % 12) + 1
        current = current.replace(year=year, month=month, day=1)
    return months