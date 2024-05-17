import pandas as pd
import yaml
import os

def compute_monthly_averages(source_dir, target_dir, columns_dir):
    csv_files = [fname for fname in os.listdir(source_dir) if fname.endswith('.csv')]
    for csv_filename in csv_files:
        input_csv_path = os.path.join(source_dir, csv_filename)
        df = pd.read_csv(input_csv_path)
        df['DATE'] = pd.to_datetime(df['DATE'])
        df['Month'] = df['DATE'].dt.month

        column_headers = df.columns
        daily_fields = []
        daily_actuals = []
        retained_columns = []
        monthly_actuals = []
        renamed_columns = []

        for header in column_headers:
            if 'Daily' in header:
                daily_actuals.append(header)
                field = header.replace('Daily', '')
                daily_fields.append(field)
            if 'Monthly' in header:
                monthly_actuals.append(header)

        txt_file_path = os.path.join(columns_dir, csv_filename.replace('.csv', '.txt'))
        with open(txt_file_path, 'r') as file:
            monthly_fields = file.read().split(',')

        for daily_field in daily_fields:
            for monthly_field in monthly_fields:
                if (monthly_field in daily_field) or ('Average' in daily_field and monthly_field.replace('Mean', '').replace('Average', '') in daily_field.replace('Average', '')):
                    retained_columns.append(daily_actuals[daily_fields.index(daily_field)])
                    renamed_columns.append(monthly_actuals[monthly_fields.index(monthly_field)])

        monthly_df = df.dropna(how='all', subset=retained_columns)[['Month'] + retained_columns]

        dtype_mapping = {col: float for col in monthly_df.columns if col != 'Month'}
        monthly_df = monthly_df.astype(dtype_mapping)
        aggregated_df = monthly_df.groupby('Month').mean().reset_index().rename(dict(zip(retained_columns, renamed_columns)), axis=1)

        output_csv_path = os.path.join(target_dir, csv_filename).replace('.csv', '_processed.csv')
        aggregated_df.to_csv(output_csv_path, index=False)

def main():
    with open("params.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
    source_dir = config["data_source"]["temp_dir"]
    columns_dir = config["data_prepare"]["dest_folder"]
    target_dir = config["data_process"]["dest_folder"]
    os.makedirs(target_dir, exist_ok=True)
    compute_monthly_averages(source_dir, target_dir, columns_dir)

if __name__ == "__main__":
    main()

