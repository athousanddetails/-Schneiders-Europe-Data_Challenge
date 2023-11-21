import pandas as pd
import argparse
from sklearn.model_selection import train_test_split
from pmdarima import auto_arima
from statsmodels.tsa.arima.model import ARIMA
import pickle 

def load_data(file_path):
    df = pd.read_csv(file_path)
    print(df.head(3))
    return df

def split_data(df):
    # TODO: Split data into training and validation sets (the test set is already provided in data/test_data.csv)
    df.set_index('StartTime', inplace=True)
    df.index = pd.DatetimeIndex(df.index).to_period('H')

    # Use the 'label' column for time-series analysis
    ts_df = df['label']
  
    # Splitting the data into train (80%) and test (20%) sets
    train_data, test_data = train_test_split(ts_df, test_size=0.2, shuffle=False)
    
    test_data.to_csv('../data/test_data.csv')
    
    return train_data

def train_model(train_data):
    # # Using auto_arima to find the best ARIMA model parameters
    auto_model = auto_arima(train_data, start_p=0, start_q=0,
                            test='adf',       # Use adf test to find optimal 'd'
                            max_p=5, max_q=5, # Maximum p and q
                            m=1,              # Frequency of the series
                            d=None,           # Let the model determine 'd'
                            seasonal=False,   # No seasonality
                            start_P=0, 
                            D=0, 
                            trace=True,
                            error_action='ignore',  
                            suppress_warnings=True, 
                            stepwise=True)
    # auto_model = auto_arima(train_data, start_p=0, start_q=0, max_p=3, max_q=3,
    #                     test='adf', d=None, seasonal=False, trace=True,
    #                     error_action='ignore', suppress_warnings=True,
    #                     stepwise=True)

    # Print the summary of the auto ARIMA model
    print(auto_model.summary())

    # Fit the ARIMA model with the parameters obtained from auto_arima
    # Replace p, d, q with the parameters obtained from auto_model
    p, d, q = auto_model.order
    model = ARIMA(train_data, order=(p, d, q))
    model = model.fit()
   
        
    return model

def save_model(model, model_file):
    # Save the model
    with open(model_file, 'wb') as file:  # Use the model_file argument
        pickle.dump(model, file)
    print(f"Model saved as {model_file}")
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Model training script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        required=True, 
        help='Path to the processed data file to train the model'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        required=True,
        help='Path to save the trained model'
    )
    return parser.parse_args()

def main(input_file, model_file):
    df = load_data(input_file)
    train_data = split_data(df)
    model = train_model(train_data)
    save_model(model, model_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file)