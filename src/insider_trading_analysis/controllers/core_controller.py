from datetime import datetime, timedelta

import pandas as pd
from api.client.services.core_service import SecClient
from models.db import DB 
from utils.utils import iterate_months
from views.plots import (plot_acquired_disposed_line_chart,
                         plot_amount_assets_acquired_disposed,
                         plot_distribution_trans_codes, plot_line_chart,
                         plot_n_most_companies_bs,
                         plot_n_most_companies_bs_by_person,
                         plot_sector_stats)

from .analysis import (companies_bs_in_period,
                       companies_bs_in_period_by_person, total_sec_acq_dis_day)
from .clean import (attach_mapping, filter_valid_exchanges, finalize,
                    remove_price_outliers)
from .flatten import normalize_transactions


class CoreController:
    def __init__(self, conf):
        self.conf = conf
        self.sec_client = SecClient(conf.base_url, conf.sec_api_key)
        self.file = DB()

    def _get_insider_transactions_it_month(self, args):
        file_name = f'insider_transactions.{args.query}_{args.start}_{args.end}'
        current_date = datetime.strptime(args.start, "%Y-%m-%d")
        next_date = current_date + timedelta(days=31)
        end_date = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=31)
        # remove time 2024-01-01 00:00:00
        current_date = str(current_date).split(' ')[0]
        start_date = str(next_date).split(' ')[0]
        end_date = str(end_date).split(' ')[0]
        for next_date in iterate_months(start_date, end_date):
            raw_iter = self.sec_client.fetch_insider_transactions(args.query, current_date, next_date)
            current_date = next_date
            df = normalize_transactions(raw_iter)
            if not df.empty:
                #self.file.csv_dump_raw(file_name, list(df.columns), list(df.values))
                self.file.df_csv_dump(file_name, df)
            #yield raw_iter

    def get_insider_transactions(self, args):
        file_name = f'insider_transactions.{args.query}_{args.start}_{args.end}'
        #file_name = 'all_trades_2022_2023'
        #if not self.file.contains(file_name):
            # if data.items >= 10,000 then fetch maxes out at 10k.
            # once from param becomes 10k, fetch ends. even if there's more data.
        #    self._get_insider_transactions_it_month(args)
        year = int(args.start.split('-')[0])
        df = self.file.df_csv_read(file_name)
        #df["total_value"] = pd.to_numeric(df["total_value"], errors="coerce")
        df["filed_at"] = pd.to_datetime(df["filed_at"], errors="coerce", utc=True)
        df["period_of_report"] = pd.to_datetime(df["period_of_report"], errors="coerce", utc=True)
        df = df[df["period_of_report"].notna()]              # remove rows that failed conversion
        df = df[df["period_of_report"].dt.year >= year]      # keep only year+ filings.
        df = df[df["filed_at"].notna()]              
        df = df[df["filed_at"].dt.year >= year]
        filter_all = (df["shares"] != df["price_per_share"]) & \
        ( (df["price_per_share"] < 6000) | (df["shares"] == 1) ) & \
        (df["total_value"] > 0) & \
        (df["code"] != "M") & \
        (df["issuer_ticker"] != "NONE") & \
        (df["issuer_ticker"] != "N/A") & \
        (df["issuer_ticker"] != "NA") & \
        (~df["issuer_cik"].astype("Int64").isin([810893,1454510,1463208,1877939,1556801,827187])) # insider incorrectly reported share price
        return df[filter_all]

    def get_exchange_mapping(self):
        file_name = 'exchange_mapping_nasdaq_nyse'
        #if not self.file.contains(file_name):
        #    mapping = self.sec_client.fetch_exchange_mapping()
        #    mapping = mapping[mapping['is_delisted'] == False]
        #    self.file.df_csv_dump(file_name, mapping)

        mapping = self.file.df_csv_read(file_name)
        return mapping
            
    def do_plot_amount_assets_acquired_disposed(self, args):
        df = self.get_insider_transactions(args)
        df = total_sec_acq_dis_day(df)
        plot_amount_assets_acquired_disposed(df, args)

    def do_plot_distribution_trans_codes(self, args):
        df = self.get_insider_transactions(args)
        plot_distribution_trans_codes(df, args)

    def do_plot_n_most_companies_bs(self, args):
        df = self.get_insider_transactions(args)
        acquired_by_ticker, disposed_by_ticker = companies_bs_in_period(df, args.year)
        plot_n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, args)

    def do_plot_n_most_companies_bs_by_person(self, args):
        df = self.get_insider_transactions(args)
        acquired_by_insider, disposed_by_insider = companies_bs_in_period_by_person(df, args.year)
        plot_n_most_companies_bs_by_person(acquired_by_insider, disposed_by_insider, args)

    def do_plot_acquired_disposed_line_chart(self, args):
        df = self.get_insider_transactions(args)
        ticker = args.ticker
        #if not self.file.contains(ticker):
        #    ohcl = self.sec_client.fetch_ticker_ohlc(ticker, args.start, args.end, '1d')
        #    self.file.df_csv_dump(ticker, ohcl, index=True)

        #Collapse duplicate daily rows
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
        ticker_filter = (df["issuer_ticker"]==ticker) & \
        (df["period_of_report"].dt.date >=start_date) & \
        (df["period_of_report"].dt.date <=end_date) #& \
        #ticker_acquired = df[ticker_filter].groupby("period_of_report")["total_value"].sum()
        ticker_acquired = df[ticker_filter].groupby("period_of_report").agg({
            "total_value": "sum",
            "acquired_disposed": "first"
        })
        
        #ticker_acquired = ticker_acquired.set_index('period_of_report')
        # remove time format 2022-03-14 00:00:00+00:00 -> 2022-03-14
        ticker_acquired.index = ticker_acquired.index.tz_convert(None)
        ticker_acquired.index.names = ['Date']


        #ticker_all = pd.merge(ohcl, ticker_acquired, on='Date', how='outer').fillna(0)
        ohcl = self.file.df_csv_read(ticker,index_col='Date', parse_dates=True)
        ohcl = ohcl.groupby(level=0).first()
        #plot_line_chart(ohcl, args)
        ticker_all = ohcl.join(ticker_acquired, how="outer")
        # forward-fill OHLC values
        # Missing values in these columns only happen on non-trading days (holidays/weekends).
        #plot_line_chart(ohcl,ticker_all, args)
        ticker_all[["Open", "High", "Low", "Close", "Volume"]] = (
        ticker_all[["Open", "High", "Low", "Close", "Volume"]].ffill()
        )
        plot_acquired_disposed_line_chart(ticker_all, f"{ticker} Stock Purchases/Sold on Daily Chart in {args.start} - {args.end}", args=args)

    def do_plot_sector_statistics(self, args):
        df = self.get_insider_transactions(args)
        sector_trades = df[['sector', 'total_value', 'period_of_report', 'acquired_disposed']].copy()
        sector_trades = sector_trades[sector_trades["sector"] != ""]
        sector_trades['period_of_report'] = pd.to_datetime(sector_trades['period_of_report'])
        sector_trades = sector_trades.set_index('period_of_report')
        sector_trades.groupby([pd.Grouper(freq='Y'), "acquired_disposed", "sector"]).head(5)

        sector_trades[sector_trades["acquired_disposed"]=="D"] \
                    .groupby([pd.Grouper(freq='Y'), "acquired_disposed", "sector"])['total_value'] \
                    .sum() \
                    .unstack()

        plot_sector_stats(sector_trades[sector_trades["acquired_disposed"]=="A"], "Insider Buy Distribution per Sector per Year", args)


    def build_dataset(self, args):
        mapping = self.get_exchange_mapping()
        filter_mapping = mapping
        ticker_symbol = args.query.split('issuer.tradingSymbol:')[-1]
        if ticker_symbol and ticker_symbol != '*:*':
            filter_mapping = mapping[mapping['issuer_ticker'] == ticker_symbol]

        df = self.get_insider_transactions(args)
        df = attach_mapping(df, filter_mapping)
        df = filter_valid_exchanges(df)
        df = remove_price_outliers(df)
        df = finalize(df)
        df.sort_values(["filed_at","issuer_ticker"], ascending=[False, True], inplace=True)
        print(f"Rows after cleaning: {len(df)}")
        out = args.output
        self.file.df_csv_dump(out, df)
        print(f"Wrote {out}")