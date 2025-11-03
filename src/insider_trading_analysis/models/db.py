import csv
class csv:
    def __init__(self, path):
        self.path = path

    def raw_dump(self, header, data):
        with open(self.path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerows(data)

    def dict_dump(self, header, data):
        with open(self.path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(data)

    def df_dump(self, df):
        df.to_csv(self.path, index=False)
        
    def read(self):
        with open(self.path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            return reader
            #for row in reader:
            #    print(row)        