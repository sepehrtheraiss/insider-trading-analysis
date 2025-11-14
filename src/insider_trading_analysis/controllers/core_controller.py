from api.client.services.core_service import SecClient
from .flatten import normalize_transactions
from .clean import attach_mapping, filter_valid_exchanges, remove_price_outliers, finalize
from .analysis import total_sec_acq_dis_day, companies_bs_in_period, companies_bs_in_period_by_person
from views.plots import plot_amount_assets_acquired_disposed, plot_distribution_trans_codes, plot_n_most_companies_bs, plot_n_most_companies_bs_by_person 
from models.db import FileHelper
from utils.utils import iterate_months
from datetime import datetime, timedelta
import pandas as pd

class CoreController:
    def __init__(self, conf):
        self.conf = conf
        self.sec_client = SecClient(conf.base_url, conf.sec_api_key)
        self.file = FileHelper()

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
        if not self.file.contains(file_name):
            # if data.items >= 10,000 then fetch maxes out at 10k.
            # once from param becomes 10k, fetch ends. even if there's more data.
            self._get_insider_transactions_it_month(args)
        year = int(args.start.split('-')[0])
        df = self.file.df_csv_read(file_name)
        #df["totalValue"] = pd.to_numeric(df["totalValue"], errors="coerce")
        df["filedAt"] = pd.to_datetime(df["filedAt"], errors="coerce", utc=True)
        df["periodOfReport"] = pd.to_datetime(df["periodOfReport"], errors="coerce", utc=True)
        df = df[df["periodOfReport"].notna()]              # remove rows that failed conversion
        df = df[df["periodOfReport"].dt.year >= year]      # keep only year+ filings.
        df = df[df["filedAt"].notna()]              
        df = df[df["filedAt"].dt.year >= year]
        filter_all = (df["shares"] != df["pricePerShare"]) & \
        ( (df["pricePerShare"] < 6000) | (df["shares"] == 1) ) & \
        (df["totalValue"] > 0) & \
        (df["code"] != "M") & \
        (df["issuerTicker"] != "NONE") & \
        (df["issuerTicker"] != "N/A") & \
        (df["issuerTicker"] != "NA") & \
        (~df["issuerCik"].astype("Int64").isin([810893,1454510,1463208,1877939,1556801,827187])) # insider incorrectly reported share price
        return df[filter_all]

    def get_exchange_mapping(self):
        file_name = 'exchange_mapping_nasdaq_nyse'
        if not self.file.contains(file_name):
            mapping = self.sec_client.fetch_exchange_mapping()
            mapping = mapping[mapping['isDelisted'] == False]
            self.file.df_csv_dump(file_name, mapping)

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

    def build_dataset(self, args):
        mapping = self.get_exchange_mapping()
        filter_mapping = mapping
        ticker_symbol = args.query.split('issuer.tradingSymbol:')[-1]
        if ticker_symbol and ticker_symbol != '*:*':
            filter_mapping = mapping[mapping['issuerTicker'] == ticker_symbol]

        df = self.get_insider_transactions(args)
        df = attach_mapping(df, filter_mapping)
        df = filter_valid_exchanges(df)
        df = remove_price_outliers(df)
        df = finalize(df)
        df.sort_values(["filedAt","issuerTicker"], ascending=[False, True], inplace=True)
        print(f"Rows after cleaning: {len(df)}")
        out = args.output
        self.file.df_csv_dump(out, df)
        print(f"Wrote {out}")