import pandas as pd
import numpy as np
from datetime import datetime
import csv
from tqdm import tqdm
import os
import gc

def get_file_size(filename):
    """Get file size in MB"""
    return os.path.getsize(filename) / (1024 * 1024)

def save_processed_data(df, base_filename='processed_transcripts', formats=None):
    """
    Save the processed DataFrame in specified formats
    formats: list of format strings ('parquet', 'csv', 'hdf')
    """
    if formats is None:
        formats = ['parquet']  # Default to parquet
    
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

def read_custom_format(filename, nrows=None, usecols=None, chunksize=100000):
    """Read file with custom delimiters: tilde (~) for columns and backtick (`) for rows"""
    file_size = get_file_size(filename)
    print(f"\nProcessing {filename} ({file_size:.1f} MB)")
    
    # Initialize empty list for DataFrames
    dfs = []
    current_chunk = []
    chunk_count = 0
    row_count = 0
    headers = None
    
    with pd.io.common.get_handle(
        filename,
        'r',
        compression='gzip',
        encoding='utf-8'
    ) as handle:
        # Read and process the header first
        header_line = ""
        while True:
            char = handle.handle.read(1)
            if not char or char == '`':
                break
            header_line += char
        
        headers = [h.strip() for h in header_line.split('~')]
        if usecols:
            col_indices = [headers.index(col) for col in usecols if col in headers]
            used_headers = [headers[i] for i in col_indices]
        else:
            col_indices = range(len(headers))
            used_headers = headers
        
        # Process the rest of the file
        print("\nProcessing rows...")
        current_row = ""
        pbar = tqdm(desc='Rows processed', unit='rows')
        
        while True:
            char = handle.handle.read(1)
            if not char:  # End of file
                break
                
            if char == '`':  # End of row
                if current_row.strip():
                    fields = current_row.split('~')
                    if usecols:
                        row_data = [fields[i] if i < len(fields) else None for i in col_indices]
                    else:
                        row_data = fields
                    current_chunk.append(row_data)
                    row_count += 1
                    pbar.update(1)
                
                # Create DataFrame when chunk size is reached
                if len(current_chunk) >= chunksize:
                    chunk_df = pd.DataFrame(current_chunk, columns=used_headers)
                    dfs.append(chunk_df)
                    current_chunk = []
                    chunk_count += 1
                    gc.collect()  # Help manage memory
                
                if nrows and row_count >= nrows:
                    break
                    
                current_row = ""
            else:
                current_row += char
        
        pbar.close()
        
        # Process any remaining rows
        if current_chunk:
            chunk_df = pd.DataFrame(current_chunk, columns=used_headers)
            dfs.append(chunk_df)
    
    print(f"\nCombining {len(dfs)} chunks...")
    final_df = pd.concat(dfs, ignore_index=True)
    print(f"Total rows processed: {len(final_df):,}")
    
    return final_df

def analyze_file_structure(filename, nrows=5):
    """Analyze the structure of the file by reading just a few rows"""
    print(f"\nAnalyzing structure of {filename}...")
    df_sample = read_custom_format(filename, nrows=nrows)
    
    print("\nColumns found:")
    for i, col in enumerate(df_sample.columns):
        print(f"{i+1}. {col}")
    
    print("\nSample data (first row):")
    for col in df_sample.columns:
        val = df_sample[col].iloc[0] if not df_sample.empty else "No data"
        print(f"\n{col}:")
        print(val if len(str(val)) < 1000 else f"{str(val)[:1000]}...")
    
    return df_sample

def process_transcript_files():
    """Process the transcript files"""
    # First analyze the structure
    print("Analyzing file structure...")
    sample_df = analyze_file_structure('1-Part-All_2024.csv.gz')
    
    print("\nWould you like to proceed with full processing? (yes/no)")
    response = input()
    
    if response.lower() != 'yes':
        print("Stopping after structure analysis")
        return sample_df
    
    dfs = []
    files = ['1-Part-All_2024.csv.gz', '2-Part-All_2024.csv.gz']
    
    print(f"\nProcessing {len(files)} files...")
    for file in tqdm(files, desc='Files'):
        # Read file with all columns
        df = read_custom_format(file)
        
        print("\nCleaning dates...")
        # Convert date columns if they exist
        date_columns = ['ANNOUNCEDDATEUTC', 'DATEOFCALLUTC']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        dfs.append(df)
        print(f"Processed {len(df):,} rows from {file}")
        gc.collect()  # Help manage memory
    
    print("\nCombining all files...")
    df = pd.concat(dfs, ignore_index=True)
    del dfs  # Free memory
    gc.collect()
    
    print("\nFinal dataset info:")
    print(df.info())
    
    # Basic statistics
    print("\nDataset Statistics:")
    print("-" * 50)
    print(f"Total rows: {len(df):,}")
    print(f"Number of unique companies: {df['COMPANYNAME'].nunique():,}")
    print(f"Number of columns: {len(df.columns)}")
    print(f"Date range: {df['DATEOFCALLUTC'].min()} to {df['DATEOFCALLUTC'].max()}")
    print("\nTypes of components:")
    print(df['TRANSCRIPTCOMPONENTTYPE'].value_counts().to_string())
    
    print("\nWould you like to save the processed data? (yes/no)")
    save_response = input()
    
    if save_response.lower() == 'yes':
        print("\nSelect formats to save (comma-separated):")
        print("1. Parquet (recommended - efficient storage and fast reading)")
        print("2. CSV (universal compatibility)")
        print("3. HDF5 (good for large datasets)")
        format_choice = input("Enter numbers (e.g., '1' or '1,2'): ")
        
        format_map = {
            '1': 'parquet',
            '2': 'csv',
            '3': 'hdf'
        }
        
        selected_formats = [format_map[f.strip()] 
                          for f in format_choice.split(',') 
                          if f.strip() in format_map]
        
        if selected_formats:
            save_results = save_processed_data(df, formats=selected_formats)
            print("\nFiles saved:")
            for fmt, path in save_results.items():
                print(f"{fmt.upper()}: {path}")
        else:
            print("No valid formats selected. Data not saved.")
    
    return df

if __name__ == "__main__":
    print("Starting transcript analysis...")
    df = process_transcript_files()
    print("\nProcessing complete!") 