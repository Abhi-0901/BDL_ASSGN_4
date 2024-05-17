import pandas as pd
from sklearn.metrics import r2_score
import os
import yaml

def calculate_r2(ground_truth_dir, predicted_dir, output_dir):
    gt_csv_files = [filename for filename in os.listdir(ground_truth_dir) if filename.endswith('.csv')]
    for csv_filename in gt_csv_files:
        gt_file_path = os.path.join(ground_truth_dir, csv_filename)
        pred_file_path = os.path.join(predicted_dir, csv_filename).replace('_prepared.csv', '_processed.csv')

        gt_dataframe = pd.read_csv(gt_file_path).dropna(axis=1, how='all')
        pred_dataframe = pd.read_csv(pred_file_path).dropna(axis=1, how='all')

        common_cols = set(gt_dataframe.columns).intersection(pred_dataframe.columns)
        gt_dataframe = gt_dataframe.dropna(subset=common_cols)
        pred_dataframe = pred_dataframe.dropna(subset=common_cols)

        common_months = set(gt_dataframe['Month']).intersection(pred_dataframe['Month'])
        gt_dataframe = gt_dataframe[gt_dataframe['Month'].isin(common_months)]
        pred_dataframe = pred_dataframe[pred_dataframe['Month'].isin(common_months)]

        r2_scores_list = []
        for col in common_cols:
            if col != 'Month':
                r2_scores_list.append(r2_score(gt_dataframe[col], pred_dataframe[col]))

        overall_r2 = 'Consistent' if all(score >= 0.9 for score in r2_scores_list) else 'Inconsistent'

        output_file_path = os.path.join(output_dir, csv_filename.replace('_prepare.csv', '_r2.txt'))
        with open(output_file_path, 'w') as file:
            file.write(overall_r2)
            file.write('\n')
            file.write(','.join([str(score) for score in r2_scores_list]))

        print(f'{csv_filename}: {overall_r2}')

def main():
    with open("params.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
    ground_truth_dir = config["data_prepare"]["dest_folder"]
    predicted_dir = config["data_process"]["dest_folder"]
    output_dir = config["evaluate"]["output"]
    os.makedirs(output_dir, exist_ok=True)
    calculate_r2(ground_truth_dir, predicted_dir, output_dir)

if __name__ == "__main__":
    main()

