import argparse
import os
import pandas as pd

def load_data(file_path):
    df = pd.read_csv(file_path)
    print(df.head(3))
    return df

def clean_data(df):
    print(df.shape)
    print(df.info())
    df_clean = df.drop_duplicates()

    
    return df_clean

def preprocess_data(df):
    # Create a dictionary to asign index codes to Countries
    country_index_dict = {'SP': 0, 'UK': 1, 'DE': 2, 'DK': 3, 'HU': 5, 'SE': 4, 'IT': 6, 'PO': 7, 'NE': 8}

    # Calculate energy surplus and agg the column "label"
    for country_code in country_index_dict.keys():
        # Calcular el surplus
        green_energy_column = f'green_energy_{country_code}.csv'
        load_column = f'Load_{country_code}'
        surplus_column = f'surplus_{country_code}'
        
        df[surplus_column] = df[green_energy_column] - df[load_column]

    # Find the country with bigger surplus and asign the code to the 'label' column
    max_surplus_country = df[[f'surplus_{country_code}' for country_code in country_index_dict.keys()]].idxmax(axis=1)
    df['label'] = max_surplus_country.apply(lambda x: country_index_dict[x.split('_')[1]])
    
    # Convert specified columns to integers
    columns_to_convert = ['green_energy_UK.csv', 'green_energy_SE.csv', 'Load_UK']
    columns_to_convert = [col for col in columns_to_convert if col in df.columns]
    for col in columns_to_convert:
        df[col] = df[col].fillna(0).astype(int)

    # Filter only the columns that don't containt "surplus"
    df_processed = df.loc[:, ~df.columns.str.contains('surplus')]
    
    return df_processed

def save_data(df, output_file):
    df.to_csv(output_file, index=False)
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data processing script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file',
        type=str,
        required=True,
        help='Path to the raw data file to process'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        required=True,
        help='Path to save the processed data'
    )
    return parser.parse_args()

def main(input_file, output_file):
    # Ensure these directories exist
    os.makedirs('../data/processed', exist_ok=True)
    
    df = load_data(input_file)
    df_clean = clean_data(df)
    df_processed = preprocess_data(df_clean)
    save_data(df_processed, output_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.output_file)