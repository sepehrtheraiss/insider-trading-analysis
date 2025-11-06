from api.client.services.core_service import SecClient
from .flatten import normalize_transactions
from .clean import attach_mapping, filter_valid_exchanges, remove_price_outliers, finalize
from models.db import FileHelper

class CoreController:
    def __init__(self, conf):
        self.conf = conf
        self.sec_client = SecClient(conf.base_url, conf.sec_api_key)
        self.file = FileHelper()

    def run_cmd(self, args):
        file_name = f'insider_transactions_{args.query}_{args.start}_{args.end}'
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

        file_name = 'exchange_mapping_nasdaq_nyse'
        if not self.file.contains(file_name):
            mapping = self.sec_client.fetch_exchange_mapping()
            self.file.df_csv_dump(file_name, mapping)

        mapping = self.file.df_csv_read(file_name)
        filter_mapping = mapping[mapping['issuerTicker'] == 'TSLA']
        #print('mapping content: ', mapping)
        df = attach_mapping(df, filter_mapping)
        df = filter_valid_exchanges(df)
        df = remove_price_outliers(df)
        df = finalize(df)
        df.sort_values(["filedAt","issuerTicker"], ascending=[False, True], inplace=True)
        print(f"Rows after cleaning: {len(df)}")
        out = args.output
        df.to_csv(out, index=False)
        print(f"Wrote {out}")        