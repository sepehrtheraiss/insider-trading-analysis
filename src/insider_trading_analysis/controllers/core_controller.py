from api.client.services.core_service import SecClient
from .flatten import normalize_transactions
from .clean import attach_mapping, filter_valid_exchanges, remove_price_outliers, finalize
from .analysis import total_sec_acq_dis_day, companies_bs_in_period
from views.plots import plot_annual_graph, plot_distribution_trans_codes, plot_n_most_companies_bs
from models.db import FileHelper
from utils.utils import iterate_months
from datetime import datetime, timedelta

class CoreController:
    def __init__(self, conf):
        self.conf = conf
        self.sec_client = SecClient(conf.base_url, conf.sec_api_key)
        self.file = FileHelper()

    def get_insider_transactions(self, args):
        file_name = f'insider_transactions.{args.query}_{args.start}_{args.end}'
        #self.file.remove(file_name)
        if not self.file.contains(file_name):
            # if data.items >= 10,000 then fetch maxes out at 10k.
            # once from param becomes 10k, fetch ends. even if there's more data.
            current_date = datetime.strptime(args.start, "%Y-%m-%d")
            next_date = current_date + timedelta(days=31)
            end_date = datetime.strptime(args.end, "%Y-%m-%d") + timedelta(days=31)
            # remove time 2024-01-01 00:00:00
            current_date = str(current_date).split(' ')[0]
            start_date = str(next_date).split(' ')[0]
            end_date = str(end_date).split(' ')[0]
            for next_date in iterate_months(start_date, end_date):
                raw_iter = self.sec_client.fetch_insider_transactions(args.query, current_date, next_date)
                self.file.json_dump(file_name, list(raw_iter))
                #self.file.json_dump_gen(file_name, raw_iter)
                current_date = next_date 
        #raw_iter = self.file.json_read_gen(file_name)
        raw_iter = self.file.json_read_lines(file_name)
        df = normalize_transactions(raw_iter)
        if df.empty:
            print("No transactions returned.")
        return df

    def get_exchange_mapping(self):
        file_name = 'exchange_mapping_nasdaq_nyse'
        if not self.file.contains(file_name):
            mapping = self.sec_client.fetch_exchange_mapping()
            mapping = mapping[mapping['isDelisted'] == False]
            self.file.df_csv_dump(file_name, mapping)

        mapping = self.file.df_csv_read(file_name)
        return mapping
            
    def do_plot_annual_graph(self, args):
        plot_annual_graph(total_sec_acq_dis_day(self.get_insider_transactions(args)))

    def do_plot_distribution_trans_codes(self, args):
        plot_distribution_trans_codes(self.get_insider_transactions(args))

    def do_plot_n_most_companies_bs(self, args):
        acquired_by_ticker, disposed_by_ticker = companies_bs_in_period(self.get_insider_transactions(args), args.year)
        plot_n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, args.year)

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