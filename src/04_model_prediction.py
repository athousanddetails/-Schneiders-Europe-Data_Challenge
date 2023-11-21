import pandas as pd
import argparse
from pmdarima import auto_arima
from statsmodels.tsa.arima.model import ARIMA
import pickle  # For saving the model
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
import json
import os
import shutil

def load_data(file_path):
    df = pd.read_csv(file_path)
    print(df.head(3))
    return df

def load_model(model_path):
    with open(model_path, 'rb') as file:
        model = pickle.load(file)
        
    return model

def make_predictions(df, model):
    # Assuming 'label' is the column you want to predict
    actual_values = df['label']

    # Forecasting
    forecast = model.get_forecast(steps=len(df))
    predictions = forecast.predicted_mean
    
    # Model Evaluation
    mse = mean_squared_error(actual_values, predictions)
    rmse = np.sqrt(mse)

    print(f'Mean Squared Error (MSE): {mse}')
    print(f'Root Mean Squared Error (RMSE): {rmse}')
    
    return predictions

def save_predictions(predictions, predictions_file):
    # Convert predictions to integers and format them as required
    formatted_predictions = {"target": {i: int(np.ceil(pred)) for i, pred in enumerate(predictions)}}
    
    # Save formatted predictions to JSON file
    with open(predictions_file, 'w') as file:
        json.dump(formatted_predictions, file)
        
    print(f"Predictions saved as {predictions_file}")
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Prediction script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        required=True,
        help='Path to the test data file to make predictions'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        required=True,
        help='Path to the trained model file'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        required=True, 
        help='Path to save the predictions'
    )
    return parser.parse_args()

def main(input_file, model_file, output_file):
    # Path to the folder
    folder_path = '../data/interim/'

    # Iterate over all the files and folders in the directory
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
    
        # Check if it's a file or folder
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.remove(file_path)  # Remove the file
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)  # Remove the folder
            
    print("All files and folders in '" + folder_path + "' have been deleted.")
    
    df = load_data(input_file)
    model = load_model(model_file)
    predictions = make_predictions(df, model)
    save_predictions(predictions, output_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file, args.output_file)
