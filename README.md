# CSV to Database ETL Pipeline

A robust and scalable ETL (Extract, Transform, Load) pipeline for processing CSV files and loading them into a database. This pipeline supports parallel processing, data validation, and error handling.

## Features

- **Extract**: Batch processing of CSV files with configurable chunk sizes
- **Transform**: Flexible data transformation and validation
- **Load**: Efficient database loading with support for multiple database types
- **Parallel Processing**: Support for processing multiple files concurrently
- **Error Handling**: Comprehensive error handling and logging
- **Data Validation**: Built-in data validation capabilities
- **Configuration Management**: YAML-based configuration
- **Logging**: Detailed logging with configurable levels and file rotation

## Prerequisites

- Python 3.8 or higher
- PostgreSQL (or other supported databases)
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/csv-to-database-pipeline.git
cd csv-to-database-pipeline
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the example configuration file:
```bash
cp config/etl_config.yaml config/config.yaml
```

2. Update the configuration file with your settings:
```yaml
database:
  type: postgresql
  host: localhost
  port: 5432
  database: your_database
  user: your_user
  password: your_password

csv:
  input_dir: ./data/input
  archive_dir: ./data/archive
  error_dir: ./data/error
  delimiter: ","
  quotechar: '"'
  encoding: utf-8
  batch_size: 10000
```

## Usage

1. Place your CSV files in the `data/input` directory

2. Run the pipeline:
```bash
python main.py --config config/config.yaml --table your_table_name
```

Optional arguments:
- `--schema`: Target database schema
- `--table`: Target table name (default: csv_data)

## Project Structure

```
csv-to-database-pipeline/
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── etl_config.yaml
├── data/
│   ├── input/
│   ├── archive/
│   └── error/
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── utils/
│   ├── __init__.py
│   ├── exceptions.py
│   └── logger.py
├── logs/
├── main.py
├── requirements.txt
└── README.md
```

## Data Validation

The pipeline supports various types of data validation:
- Not null constraints
- Data type validation
- Range validation
- Custom validation rules

## Error Handling

- Failed records are logged with detailed error messages
- Failed files are moved to the error directory
- Successful files are archived after processing
- Comprehensive logging of all operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with Python and SQLAlchemy
- Uses pandas for data manipulation
- Inspired by real-world ETL challenges

## Support

For support, please open an issue in the GitHub repository or contact the maintainers. 