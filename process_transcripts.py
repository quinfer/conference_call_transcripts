import pandas as pd
import numpy as np
import os
import gc
from tqdm import tqdm
from pathlib import Path

def get_file_size(file_path):
    """Get file size in MB"""
    return os.path.getsize(file_path) / (1024 * 1024)

def analyze_file_structure(file_path):
    """Analyze the structure of a CSV file"""
    print(f"\nAnalyzing {file_path} (Size: {get_file_size(file_path):.1f} MB)")
    
    # Read just 5 rows to get structure
    sample = pd.read_csv(file_path, sep='~', lineterminator='`', compression='gzip', nrows=5)
    print("\nColumns found:")
    for col in sample.columns:
        print(f"- {col}")
    
    return sample

def read_custom_format(file_path, chunk_size=100_000):
    """Read CSV file in chunks with progress tracking using custom delimiters"""
    total_rows = 0
    chunks = []
    
    # Count total rows for progress bar
    print(f"\nCounting rows in {file_path}...")
    for chunk in pd.read_csv(file_path, sep='~', lineterminator='`', compression='gzip', 
                           chunksize=chunk_size, on_bad_lines='skip', 
                           quoting=3, dtype=str):  # quoting=3 disables quote parsing
        total_rows += len(chunk)
    
    print(f"Total rows to process: {total_rows:,}")
    
    # Process chunks with progress bar
    with tqdm(total=total_rows, desc='Processing rows') as pbar:
        for chunk in pd.read_csv(file_path, sep='~', lineterminator='`', compression='gzip', 
                               chunksize=chunk_size, on_bad_lines='skip',
                               quoting=3, dtype=str):  # quoting=3 disables quote parsing
            chunks.append(chunk)
            pbar.update(len(chunk))
            
            # Force garbage collection after each chunk
            gc.collect()
    
    print("\nCombining chunks...")
    return pd.concat(chunks, ignore_index=True)

def process_transcripts(file_path):
    """Process transcript data from a custom-formatted CSV file"""
    # Read the data using the custom format function
    df = read_custom_format(file_path)
    
    # Example processing: Print the first few rows
    print("\nSample of the processed data:")
    print(df.head())

def save_processed_data(df, base_filename='processed_transcripts', formats=None):
    """Save the processed DataFrame in specified formats"""
    if formats is None:
        formats = ['parquet']
        
    results = {}
    os.makedirs('processed_data', exist_ok=True)
    
    for fmt in formats:
        output_path = f"processed_data/{base_filename}.{fmt}"
        print(f"\nSaving to {output_path}...")
        
        if fmt == 'parquet':
            df.to_parquet(output_path, index=False)
            results['parquet'] = output_path
            
        elif fmt == 'csv':
            df.to_csv(output_path, index=False)
            results['csv'] = output_path
            
        elif fmt == 'hdf':
            df.to_hdf(output_path, key='transcripts', mode='w')
            results['hdf'] = output_path
            
        file_size = get_file_size(output_path)
        print(f"Saved {fmt.upper()} file: {file_size:.1f} MB")
    
    return results

def main():
    # Get all .csv.gz files from raw_data directory
    raw_data_dir = Path('raw_data')
    files = list(raw_data_dir.glob('*.csv.gz'))
    
    if not files:
        print("No .csv.gz files found in raw_data directory!")
        print("Please place your input files in the raw_data directory.")
        return
    
    print(f"Found {len(files)} files to process:")
    for file in files:
        print(f"- {file.name}")
    
    # Analyze first file structure
    print("\nAnalyzing file structure...")
    sample_df = analyze_file_structure(files[0])
    
    print("\nWould you like to proceed with full processing? (yes/no)")
    response = input().lower().strip()
    if response != 'yes':
        print("Processing cancelled.")
        return
    
    # Process all files
    dfs = []
    print(f"\nProcessing {len(files)} files...")
    for file in tqdm(files, desc='Files'):
        print(f"\nProcessing {file.name}...")
        df = read_custom_format(file)
        
        print("\nCleaning dates...")
        # Convert date columns if they exist
        date_columns = ['ANNOUNCEDDATEUTC', 'DATEOFCALLUTC']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        dfs.append(df)
        
        # Force garbage collection after each file
        gc.collect()
    
    print("\nCombining all files...")
    df = pd.concat(dfs, ignore_index=True)
    
    # Print summary statistics
    print(f"\nProcessing complete!")
    print(f"Total rows: {len(df):,}")
    print(f"Number of unique companies: {df['COMPANYNAME'].nunique():,}")
    print(f"Number of columns: {len(df.columns)}")
    print(f"Date range: {df['DATEOFCALLUTC'].min()} to {df['DATEOFCALLUTC'].max()}")
    
    print("\nTypes of components:")
    print(df['TRANSCRIPTCOMPONENTTYPE'].value_counts().to_string())
    
    # Save processed data
    print("\nWould you like to save the processed data? (yes/no)")
    response = input().lower().strip()
    if response == 'yes':
        print("\nSelect output format(s) (comma-separated):")
        print("Available formats: parquet, csv, hdf")
        formats = input().lower().strip().split(',')
        formats = [fmt.strip() for fmt in formats if fmt.strip() in ['parquet', 'csv', 'hdf']]
        
        if formats:
            save_processed_data(df, formats=formats)
        else:
            print("No valid formats selected. Saving as parquet by default...")
            save_processed_data(df)
    
    print("\nProcessing pipeline complete!")

if __name__ == "__main__":
    main() 