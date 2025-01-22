import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm

def load_transcript_data(file_path='processed_data/processed_transcripts.parquet'):
    """Load the processed transcript data"""
    print(f"Loading data from {file_path}...")
    return pd.read_parquet(file_path)

def analyze_basic_stats(df):
    """Analyze basic statistics of the dataset"""
    print("\n=== Basic Dataset Statistics ===")
    print("-" * 50)
    print(f"Total number of rows: {len(df):,}")
    print(f"Number of unique companies: {df['COMPANYNAME'].nunique():,}")
    print(f"Date range: {df['DATEOFCALLUTC'].min()} to {df['DATEOFCALLUTC'].max()}")
    
    # Component types analysis
    print("\nTranscript Component Types:")
    comp_types = df['TRANSCRIPTCOMPONENTTYPE'].value_counts()
    print(comp_types.to_string())
    
    # Speaker types analysis
    print("\nSpeaker Types:")
    speaker_types = df['SPEAKERTYPE'].value_counts()
    print(speaker_types.to_string())

def analyze_temporal_patterns(df):
    """Analyze temporal patterns in the transcripts"""
    print("\n=== Temporal Analysis ===")
    print("-" * 50)
    
    # Convert to datetime if not already
    df['DATEOFCALLUTC'] = pd.to_datetime(df['DATEOFCALLUTC'])
    
    # Calls per month
    monthly_calls = df.groupby(['COMPANYNAME', pd.Grouper(key='DATEOFCALLUTC', freq='M')]).size().reset_index()
    calls_per_month = monthly_calls.groupby('DATEOFCALLUTC').size()
    
    print("\nCalls per month:")
    print(calls_per_month.to_string())
    
    # Most active months
    print("\nMost active months:")
    print(calls_per_month.nlargest(5).to_string())

def analyze_company_patterns(df):
    """Analyze company-specific patterns"""
    print("\n=== Company Analysis ===")
    print("-" * 50)
    
    # Companies with most calls
    company_calls = df.groupby('COMPANYNAME').size().sort_values(ascending=False)
    print("\nTop 10 Companies by Number of Transcript Components:")
    print(company_calls.head(10).to_string())
    
    # Average call length (by component count) per company
    calls_per_company = df.groupby(['COMPANYNAME', 'TRANSCRIPTID']).size()
    avg_length = calls_per_company.groupby('COMPANYNAME').mean().sort_values(ascending=False)
    print("\nTop 10 Companies by Average Call Length (component count):")
    print(avg_length.head(10).to_string())

def analyze_content_patterns(df):
    """Analyze patterns in the transcript content"""
    print("\n=== Content Analysis ===")
    print("-" * 50)
    
    # Sample size for text analysis (to avoid memory issues)
    sample_size = min(10000, len(df))
    
    # Average text length by component type
    df['text_length'] = df['COMPONENTTEXT'].str.len()
    avg_length = df.groupby('TRANSCRIPTCOMPONENTTYPE')['text_length'].agg(['mean', 'min', 'max'])
    print("\nText Length Statistics by Component Type:")
    print(avg_length.round(2).to_string())
    
    # Most common speaker roles
    print("\nMost Common Speaker Roles:")
    speaker_roles = df['SPEAKERTYPE'].value_counts()
    print(speaker_roles.head(10).to_string())

def main():
    # Load the data
    df = load_transcript_data()
    
    # Run analyses
    analyze_basic_stats(df)
    analyze_temporal_patterns(df)
    analyze_company_patterns(df)
    analyze_content_patterns(df)
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main() 