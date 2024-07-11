import os
import pandas as pd

def csv_to_parquet(csv_file_path, parquet_file_path, engine='pyarrow'):
    """
    Convert a CSV file to a Parquet file.
    
    :param csv_file_path: Path to the input CSV file.
    :param parquet_file_path: Path to the output Parquet file.
    :param engine: Engine to use for Parquet ('pyarrow' or 'fastparquet').
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        # Convert to Parquet
        df.to_parquet(parquet_file_path, engine=engine)
        
        print(f"CSV file '{csv_file_path}' has been successfully converted to Parquet file '{parquet_file_path}'.")
    except Exception as e:
        print(f"An error occurred while converting '{csv_file_path}': {e}")

def convert_all_csv_in_directory(directory, output_directory, engine='pyarrow'):
    """
    Convert all CSV files in a given directory to Parquet files.
    
    :param directory: Path to the directory containing CSV files.
    :param output_directory: Path to the directory where Parquet files will be saved.
    :param engine: Engine to use for Parquet ('pyarrow' or 'fastparquet').
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            csv_file_path = os.path.join(directory, filename)
            parquet_file_path = os.path.join(output_directory, filename.replace('.csv', '.parquet'))
            
            # Convert CSV to Parquet
            csv_to_parquet(csv_file_path, parquet_file_path, engine)

# Example usage
directory = '/home/haseebullah/My Files/XLoop/deg team/data/citibike/csv_chunks/06'
output_directory = '/home/haseebullah/My Files/XLoop/deg team/data/citibike/parquet_chunks/06'
convert_all_csv_in_directory(directory, output_directory)
