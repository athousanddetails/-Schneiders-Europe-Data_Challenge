import argparse
import datetime
import pandas as pd
import os
import glob
import re
from utils import perform_get_request, xml_to_load_dataframe, xml_to_gen_data

def get_load_data_from_entsoe(regions, periodStart='202201010000', periodEnd='202301010000', output_path='../data/interim/'):
    
    # TODO: There is a period range limit of 1 year for this API. Process in 1 year chunks if needed
    
    # URL of the RESTful API
    url = 'https://web-api.tp.entsoe.eu/api'

    # General parameters for the API
    # Refer to https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_documenttype
    params = {
        'securityToken': 'b5b8c21b-a637-4e17-a8fe-0d39a16aa849',
        'documentType': 'A65',
        'processType': 'A16',
        'outBiddingZone_Domain': 'FILL_IN', # used for Load data
        'periodStart': periodStart, # in the format YYYYMMDDHHMM
        'periodEnd': periodEnd # in the format YYYYMMDDHHMM
    }

    # Loop through the regions and get data for each region
    for region, area_code in regions.items():
        print(f'Fetching data for {region}...')
        params['outBiddingZone_Domain'] = area_code
    
        # Use the requests library to get data from the API for the specified time range
        response_content = perform_get_request(url, params)

        # Response content is a string of XML data
        df = xml_to_load_dataframe(response_content)

        # Save the DataFrame to a CSV file
        df.to_csv(f'{output_path}/load_{region}.csv', index=False)
       
    return

def get_gen_data_from_entsoe(regions, periodStart='202201010000', periodEnd='202301010000', output_path='../data/interim/'):
    
    # TODO: There is a period range limit of 1 day for this API. Process in 1 day chunks if needed

    # URL of the RESTful API
    url = 'https://web-api.tp.entsoe.eu/api'

    # General parameters for the API
    params = {
        'securityToken': 'b5b8c21b-a637-4e17-a8fe-0d39a16aa849',
        'documentType': 'A75',
        'processType': 'A16',
        'outBiddingZone_Domain': 'FILL_IN', # used for Load data
        'in_Domain': 'FILL_IN', # used for Generation data
        'periodStart': periodStart, # in the format YYYYMMDDHHMM
        'periodEnd': periodEnd # in the format YYYYMMDDHHMM
    }

    # Loop through the regions and get data for each region
    for region, area_code in regions.items():
        print(f'Fetching data for {region}...')
        params['outBiddingZone_Domain'] = area_code
        params['in_Domain'] = area_code
    
        # Use the requests library to get data from the API for the specified time range
        response_content = perform_get_request(url, params)

        # Response content is a string of XML data
        dfs = xml_to_gen_data(response_content)

        # Save the dfs to CSV files
        for psr_type, df in dfs.items():
            # Save the DataFrame to a CSV file
            df.to_csv(f'{output_path}/gen_{region}_{psr_type}.csv', index=False)
    
    return


def parse_arguments():
    parser = argparse.ArgumentParser(description='Data ingestion script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--start_time', 
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), 
        default=datetime.datetime(2022, 1, 1), 
        help='Start time for the data to download, format: YYYY-MM-DD'
    )
    parser.add_argument(
        '--end_time', 
        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), 
        default=datetime.datetime(2023, 1, 1), 
        help='End time for the data to download, format: YYYY-MM-DD'
    )
    parser.add_argument(
        '--output_path', 
        type=str, 
        required=True,
        help='Full file path for the output CSV file, including the filename'
    )
    return parser.parse_args()

def process_file(file_path):
    # Read the file
    df = pd.read_csv(file_path)

    # Remove duplicates
    df = df.drop_duplicates(subset=['StartTime'])
    
    # Modify StartTime and EndTime columns
    for col in ['StartTime', 'EndTime']:
        df[col] = df[col].str.replace('Z', '').str.replace('T', ' ')
        df[col] = pd.to_datetime(df[col])

    # Define aggregation dictionary
    agg_dict = {
        'EndTime': 'last', 
        'AreaID': 'first', 
        'UnitName': 'first'
    }

    # Add 'PsrType' to aggregation dictionary if it exists in the DataFrame
    if 'PsrType' in df.columns:
        agg_dict['PsrType'] = 'first'

    # Add 'quantity' or 'Load' to aggregation dictionary based on file type
    if 'gen_' in file_path:
        df['quantity'] = df['quantity'].astype(int)
        agg_dict['quantity'] = 'sum'
    elif 'load_' in file_path:
        df['Load'] = df['Load'].astype(int)
        agg_dict['Load'] = 'sum'

    # Resample
    df.set_index('StartTime', inplace=True)
    df = df.resample('1H').agg(agg_dict)
    
    # Delete rows where EndTime is NaN
    df = df.dropna(subset=['EndTime'])

    # Interpolate missing values for 'quantity' or 'Load'
    if 'quantity' in agg_dict:
        df['quantity'] = df['quantity'].interpolate()
    if 'Load' in agg_dict:
        df['Load'] = df['Load'].interpolate()
        
    # Round EndTime to the nearest hour after interpolation
    df = df.drop(df[df['EndTime'].dt.minute > 0].index)

    # Save or return processed DataFrame
    df.to_csv(file_path.replace('.csv', '_processed.csv'))

def main(start_time, end_time, output_path):
    interim_path = '../data/interim/'
    print(f"Output Path: {output_path}")
    print(f"Interim Path: {interim_path}")

    # Ensure these directories exist
    os.makedirs(interim_path, exist_ok=True)
    
    regions = {
        'HU': '10YHU-MAVIR----U',
        'IT': '10YIT-GRTN-----B',
        'PO': '10YPL-AREA-----S',
        'SP': '10YES-REE------0',
        'UK': '10Y1001A1001A92E',
        'DE': '10Y1001A1001A83F',
        'DK': '10Y1001A1001A65H',
        'SE': '10YSE-1--------K',
        'NE': '10YNL----------L',
    }

    # Transform start_time and end_time to the format required by the API
    start_time = start_time.strftime('%Y%m%d%H%M')
    end_time = end_time.strftime('%Y%m%d%H%M')

    # Get Load and Generation data from ENTSO-E
    get_load_data_from_entsoe(regions, start_time, end_time, interim_path)
    get_gen_data_from_entsoe(regions, start_time, end_time, interim_path)

    # Process each file

    for file in glob.glob('../data/interim/gen_*.csv') + glob.glob('../data/interim/load_*.csv'):
        process_file(file)

    # Directory containing the files
    directory = "../data/interim/"

    # Get a list of all relevant files
    all_files = [f for f in os.listdir(directory) if re.match(r'gen_[A-Z]{2}_.+_processed\.csv', f)]

    # Group files by country code
    files_by_country = {}
    for file in all_files:
        country_code = file.split('_')[1]
        files_by_country.setdefault(country_code, []).append(os.path.join(directory, file))

    # Process each group of files
    for country_code, files in files_by_country.items():
        # Read and combine the files for each country code
        combined_df = pd.concat([pd.read_csv(file) for file in files])

        # Drop the columns 'PsrType' and 'UnitName'
        combined_df = combined_df.drop(columns=['PsrType', 'UnitName'])

        # Group by 'StartTime' and sum the 'quantity'
        grouped_df = combined_df.groupby(['StartTime', 'EndTime', "AreaID"])['quantity'].sum().reset_index()

        # Save the grouped data to a new CSV file for each country code
        output_file_path = os.path.join(directory, f"combined_gen_{country_code}.csv")
        grouped_df.to_csv(output_file_path, index=False)
        print(f"The Files weresaved to: {interim_path}")

    # Get a list of all combined files
    combined_files = [f for f in os.listdir(directory) if f.startswith('combined_gen') and f.endswith('.csv')]

    # Create an empty DataFrame for final results
    final_df = pd.DataFrame()

    # Process each combined file
    for combined_file in combined_files:
        # Read the combined file
        combined_df = pd.read_csv(os.path.join(directory, combined_file))

        # Get the country code from the filename
        country_code = combined_file.split('_')[2]

        # Rename the 'quantity' column with the country code
        combined_df.rename(columns={'quantity': f'green_energy_{country_code}'}, inplace=True)

        # Select only necessary columns
        combined_df = combined_df[['StartTime', 'EndTime', f'green_energy_{country_code}']]

        # Add the column to the final DataFrame
        if final_df.empty:
            final_df = combined_df
        else:
            final_df = pd.merge(final_df, combined_df, on=['StartTime', 'EndTime'], how='outer')

    # Save the final DataFrame to a new CSV file
    output_file_path = os.path.join(interim_path, 'final_combined_data.csv')
    final_df.to_csv(output_file_path, index=False)
    print(f"Final combined data saved to: {interim_path}")

    # Load the final_combined_data
    final_combined_data_path = os.path.join(interim_path, 'final_combined_data.csv')
    final_combined_data = pd.read_csv(final_combined_data_path)

    # Define the path where the load files are located
    load_files_path = '../data/interim/'

    # List all load_* files
    load_files = [f for f in os.listdir(load_files_path) if f.startswith('load_') and f.endswith('_processed.csv')]

    # Function to extract country code from filename
    def extract_country_code(filename):
        return filename.split('_')[1].upper()

    # Iterate over each load file, merge it with the final_combined_data
    for file in load_files:
        country_code = extract_country_code(file)
        load_file_path = os.path.join(load_files_path, file)
        load_data = pd.read_csv(load_file_path)

        # Rename the 'Load' column to 'Load_CountryCode'
        load_data.rename(columns={'Load': f'Load_{country_code}'}, inplace=True)

        # Merge with final_combined_data
        final_combined_data = final_combined_data.merge(load_data[['StartTime', 'EndTime', f'Load_{country_code}']],
                                                        on=['StartTime', 'EndTime'], 
                                                        how='left')

    # Fill Na with 0
    final_combined_data.fillna(0, inplace=True)
    
    # Save the merged data to a new CSV file
    final_combined_data.to_csv(output_path, index=False)
    print(f'Merged data saved to {output_path}')

if __name__ == "__main__":
    args = parse_arguments()
    main(args.start_time, args.end_time, args.output_path)