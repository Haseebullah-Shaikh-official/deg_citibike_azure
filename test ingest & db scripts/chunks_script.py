import os
import pandas as pd

def split_csv(input_file, num_chunks):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file)
    
    # Calculate the size of each chunk
    chunk_size = len(df) // num_chunks
    remainder = len(df) % num_chunks
    
    # Split the DataFrame into chunks and save each chunk to a separate file
    for i in range(num_chunks):
        start_idx = i * chunk_size + min(i, remainder)
        end_idx = (i + 1) * chunk_size + min(i + 1, remainder)
        chunk = df[start_idx:end_idx]
        chunk_file_name = f"{os.path.splitext(input_file)[0]}_{i+1:02}.csv"
        chunk.to_csv(chunk_file_name, index=False)
        print(f"Chunk {i+1} saved as {chunk_file_name}")


# Example usage
input_file = "./06/06_CITIBIKE_ZERO_TO_SNOWFLAKE.csv"
chunk_size = 4  # Set your desired chunk size here
split_csv(input_file, chunk_size)
