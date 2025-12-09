from datetime import datetime
from dateutil.relativedelta import relativedelta

# Prettify y axis: 2000000 to $2M
def millions_formatter(x,y=0):
    return '$ {:,.0f} M'.format(x*1e-6)


def iterate_months(start_date: str, end_date: str):
    """Yield (month_start, month_end) tuples from start_date to end_date inclusive."""
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        # Start of month (always current)
        month_start = current.replace(day=1)

        # End of month: (next_month - 1 day)
        next_month = month_start + relativedelta(months=1)
        month_end = next_month - relativedelta(days=1)

        # Clamp month_end to overall end_date
        if month_end > end:
            month_end = end

        yield month_start.date().isoformat(), month_end.date().isoformat()

        # Move to first day of next month
        current = next_month
