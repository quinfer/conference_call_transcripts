# Conference Call Transcript Analysis Pipeline

This repository contains a pipeline for processing and analyzing conference call transcripts. The pipeline consists of two main components:
1. A data processing script that efficiently handles large CSV files and converts them to optimized formats
2. An analysis script that provides comprehensive statistics and insights about the transcript data

## Prerequisites

- Python 3.8 or higher
- Required Python packages:
```
pandas
numpy
matplotlib
seaborn
tqdm
pyarrow  # for parquet support
tables   # for HDF5 support
```

Install the required packages using:
```bash
pip install -r requirements.txt
```

## Data Structure

The pipeline expects input data in CSV format (can be gzipped) with the following key columns:
- COMPANYNAME: Name of the company
- COMPANYID: Unique identifier for the company
- TRANSCRIPTID: Unique identifier for each transcript
- DATEOFCALLUTC: UTC timestamp of the call
- ANNOUNCEDDATEUTC: UTC timestamp of call announcement
- TRANSCRIPTCOMPONENTTYPE: Type of transcript component (e.g., Answer, Question)
- SPEAKERTYPE: Type of speaker (e.g., Executives, Analysts)
- COMPONENTTEXT: The actual text content

## Pipeline Steps

### 1. Data Processing

Run the processing script to convert raw CSV files to optimized formats:

```bash
python process_transcripts.py
```

This script will:
- Load and process CSV files in memory-efficient chunks
- Clean and standardize date formats
- Save the processed data in multiple formats (default: Parquet)
- Create a 'processed_data' directory for outputs

### 2. Data Analysis

After processing, run the analysis script:

```bash
python analyze_transcripts.py
```

This will generate statistics about:
- Basic dataset metrics (row counts, unique companies)
- Temporal patterns (calls per month)
- Company-specific patterns
- Content analysis (text length by component type)

## Output Structure

```
conference_call_transcripts/
├── processed_data/
│   └── processed_transcripts.parquet
├── process_transcripts.py
├── analyze_transcripts.py
├── requirements.txt
├── LICENSE
└── README.md
```

## Memory Considerations

The processing script uses chunked reading and garbage collection to handle large files efficiently. For very large datasets:
- Adjust the `chunk_size` parameter in `process_transcripts.py` (default: 100,000 rows)
- Monitor system memory usage during processing
- Consider processing files individually if handling multiple large files

## Customization

### Processing Options
- Modify the `save_processed_data()` function to change output formats
- Adjust date column handling in the processing script
- Configure chunk sizes for memory optimization

### Analysis Options
- Modify analysis functions in `analyze_transcripts.py` for custom metrics
- Adjust sample sizes for text analysis
- Add visualization functions as needed

## Troubleshooting

Common issues and solutions:
1. Memory errors: Reduce chunk size in processing script
2. Missing columns: Verify input CSV structure matches expected format
3. Date parsing errors: Check date format consistency in input files

## Contributing

We welcome contributions to improve the pipeline! Here's how you can help:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/improvement`)
3. Make your changes
4. Run any existing tests
5. Commit your changes (`git commit -am 'Add new feature'`)
6. Push to the branch (`git push origin feature/improvement`)
7. Create a Pull Request

Please make sure to update tests as appropriate and follow the existing coding style.

### Development Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/conference_call_transcripts.git
cd conference_call_transcripts
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Citation

If you use this pipeline in your research, please cite:
```
@misc{conference_call_analysis,
  title={Conference Call Transcript Analysis Pipeline},
  year={2024},
  url={https://github.com/yourusername/conference_call_transcripts}
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 