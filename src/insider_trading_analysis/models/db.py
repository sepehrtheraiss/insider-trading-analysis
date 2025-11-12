import os
import csv
import json
import pandas as pd

class FileHelper:
    def __init__(self, path="out"):
        self.path = path
        os.makedirs(path, exist_ok=True)

    def contains(self, file_name):
        return os.path.exists(f"{self.path}/{file_name}")
    
    def remove(self, file_name):
        if self.contains(file_name):
            os.remove(f"{self.path}/{file_name}")
    
    def dump(self, file_name, gen):
        with open(f"{self.path}/{file_name}", "w") as f:
            for row in gen:
                f.write(str(row))

    def read(self, file_name):
        with open(f"{self.path}/{file_name}", "r") as f:
            for row in f:
                yield row

    def json_dump(self, file_name, data):
        with open(f"{self.path}/{file_name}", 'a') as json_file:
            json.dump(data, json_file, indent=4)
            json_file.write('\n')

    def json_dump_gen(self, file_name, gen):
        with open(f"{self.path}/{file_name}", 'a') as json_file:
            json_file.write('[')
            first = True
            for row in gen:
                if not first:
                    # generators read per indicies {}
                    json_file.write(',\n')
                json.dump(row, json_file, indent=4)
                first = False
            json_file.write(']')

    def json_read(self, file_name):
        with open(f"{self.path}/{file_name}", "r") as f:
            return json.load(f)        

    def json_read_gen(self, file_name):
        with open(f"{self.path}/{file_name}", "r") as f:
            for row in json.load(f):
                yield row

    def json_read_lines(self, file_name):
        with open(f"{self.path}/{file_name}", "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
                
    def df_csv_dump(self, file_name, df, index=False):
        df.to_csv(f"{self.path}/{file_name}", index=index)

    def df_csv_read(self, file_name):
        return pd.read_csv(f"{self.path}/{file_name}")     
    
    def csv_dump_raw(self, file_name, header, data):
        with open(f"{self.path}/{file_name}", 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerows(data)

    def csv_dump_dict(self, file_name, header, data):
        with open(f"{self.path}/{file_name}", 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(data)

    def csv_read(self, file_name):
        with open(f"{self.path}/{file_name}", 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            return reader   
          
        