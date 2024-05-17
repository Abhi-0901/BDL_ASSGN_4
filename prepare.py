import pandas as pd
import yaml
import os

def generate_monthly_aggregates(input_folder, output_folder):
    csv_filenames = [fname for fname in os.listdir(input_folder) if fname.endswith('.csv')]
    for csv_name in csv_filenames:
        input_csv_path = os.path.join(input_folder, csv_name)
        df = pd.read_csv(input_csv_path)
        df['DATE'] = pd.to_datetime(df['DATE'])
        df['Month'] = df['DATE'].dt.month

        col_headers = df.columns
        monthly_fields = []
        actual_monthly_fields = []
        for header in col_headers:
            if 'Monthly' in header:
                actual_monthly_fields.append(header)
                field = header.replace('Monthly', '')
                if 'WetBulb' not in field and 'Departure' not in field:
                    field = field.replace('Temperature', 'DryBulbTemperature')
                monthly_fields.append(field)

        monthly_df = df.dropna(how='all', subset=actual_monthly_fields)[['Month'] + actual_monthly_fields]
        output_csv_path = os.path.join(output_folder, csv_name).replace('.csv', '_prepared.csv')
        monthly_df.to_csv(output_csv_path, index=False)

        txt_output_path = os.path.join(output_folder, csv_name.replace('.csv', '.txt'))
        with open(txt_output_path, 'w') as txt_file:
            txt_file.write(','.join(monthly_fields))

def main():
    with open("params.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
    input_folder = config["data_source"]["temp_dir"]
    output_folder = config["data_prepare"]["dest_folder"]
    os.makedirs(output_folder, exist_ok=True)
    generate_monthly_aggregates(input_folder, output_folder)

if __name__ == "__main__":
    main()

