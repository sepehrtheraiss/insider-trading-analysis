from api.client.services.core_service import SecClient
from .flatten import normalize_transactions
from .clean import attach_mapping, filter_valid_exchanges, remove_price_outliers, finalize
from .analysis import total_sec_acq_dis_day, companies_bs_in_period
from views.plots import plot_annual_graph, distribution_trans_codes, n_most_companies_bs
from models.db import FileHelper

class CoreController:
    def __init__(self, conf):
        self.conf = conf
        self.sec_client = SecClient(conf.base_url, conf.sec_api_key)
        self.file = FileHelper()

    def build_dataset(self, args):
        file_name = f'insider_transactions_{args.query}_{args.start}_{args.end}'
        ticker_symbol = args.query.split('issuer.tradingSymbol:')[-1]
        #self.file.remove(file_name)
        if not self.file.contains(file_name):
            raw_iter = self.sec_client.fetch_insider_transactions(args.query, start=args.start, end=args.end)
            self.file.json_dump_gen(file_name, raw_iter)
            #self.file.json_dump(file_name, list(raw_iter))

        raw_iter = self.file.json_read_gen(file_name)
        df = normalize_transactions(raw_iter)
        if df.empty:
            print("No transactions returned.")
            return
        
        #plot_annual_graph(total_sec_acq_dis_day(df))
        #distribution_trans_codes(df)
        #acquired_by_ticker, disposed_by_ticker = companies_bs_in_period(df, 2024)
        #n_most_companies_bs(acquired_by_ticker, disposed_by_ticker, 2024)

        file_name = 'exchange_mapping_nasdaq_nyse'
        if not self.file.contains(file_name):
            mapping = self.sec_client.fetch_exchange_mapping()
            mapping = mapping[mapping['isDelisted'] == False]
            self.file.df_csv_dump(file_name, mapping)

        mapping = self.file.df_csv_read(file_name)
        filter_mapping = mapping
        if ticker_symbol:
            filter_mapping = mapping[mapping['issuerTicker'] == ticker_symbol]
        df = attach_mapping(df, filter_mapping)
        df = filter_valid_exchanges(df)
        df = remove_price_outliers(df)
        df = finalize(df)
        df.sort_values(["filedAt","issuerTicker"], ascending=[False, True], inplace=True)
        print(f"Rows after cleaning: {len(df)}")
        out = args.output
        self.file.df_csv_dump(out, df)
        print(f"Wrote {out}")