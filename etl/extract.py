import os
import csv
import pandas as pd
import logging
from typing import Dict, List, Any, Optional, Tuple, Generator
import shutil
from datetime import datetime
import concurrent.futures
from pathlib import Path

from utils.exceptions import ExtractionError, ValidationError

class Extractor:
    """Extract data from CSV files."""
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize extractor with configuration.
        
        Args:
            config: Extraction configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        self.input_dir = Path(config["input_dir"])
        self.archive_dir = Path(config["archive_dir"])
        self.error_dir = Path(config["error_dir"])
        self.delimiter = config["delimiter"]
        self.quotechar = config["quotechar"]
        self.encoding = config["encoding"]
        self.batch_size = config["batch_size"]
    
    def list_csv_files(self) -> List[Path]:
        """
        List all CSV files in the input directory.
        
        Returns:
            List of CSV file paths
        """
        self.logger.info(f"Searching for CSV files in {self.input_dir}")
        
        if not self.input_dir.exists():
            self.logger.error(f"Input directory {self.input_dir} does not exist")
            raise ExtractionError(f"Input directory {self.input_dir} does not exist")
        
        csv_files = list(self.input_dir.glob("*.csv"))
        self.logger.info(f"Found {len(csv_files)} CSV files in {self.input_dir}")
        
        return csv_files
    
    def get_csv_schema(self, file_path: Path) -> List[str]:
        """
        Get the column names from a CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of column names
        """
        try:
            with open(file_path, 'r', encoding=self.encoding) as f:
                reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
                header = next(reader)
                return [col.strip() for col in header]
        except Exception as e:
            self.logger.error(f"Failed to read schema from {file_path}: {str(e)}")
            raise ExtractionError(f"Failed to read schema from {file_path}: {str(e)}")
    
    def validate_schema(self, schema: List[str], expected_schema: Optional[List[str]] = None) -> bool:
        """
        Validate CSV schema against expected schema.
        
        Args:
            schema: Actual schema
            expected_schema: Expected schema (optional)
            
        Returns:
            True if schema is valid
            
        Raises:
            ValidationError: If schema is invalid
        """
        # If no expected schema provided, just check that we have columns
        if not expected_schema:
            if not schema or len(schema) == 0:
                raise ValidationError("CSV has no columns")
            return True
        
        # Check if all expected columns are present
        missing_columns = set(expected_schema) - set(schema)
        if missing_columns:
            raise ValidationError(f"Missing expected columns: {missing_columns}")
        
        return True
    
    def extract_from_file(self, file_path: Path, expected_schema: Optional[List[str]] = None) -> Generator[pd.DataFrame, None, None]:
        """
        Extract data from a CSV file in batches.
        
        Args:
            file_path: Path to CSV file
            expected_schema: Expected column schema (optional)
            
        Yields:
            Pandas DataFrames containing batches of data
        """
        self.logger.info(f"Extracting data from {file_path}")
        
        try:
            # Validate the file schema
            schema = self.get_csv_schema(file_path)
            self.validate_schema(schema, expected_schema)
            
            # Read the CSV in chunks
            reader = pd.read_csv(
                file_path,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                encoding=self.encoding,
                chunksize=self.batch_size,
                low_memory=False
            )
            
            for i, chunk in enumerate(reader):
                self.logger.debug(f"Extracted batch {i+1} from {file_path}")
                yield chunk
                
            self.logger.info(f"Completed extraction from {file_path}")
            
        except Exception as e:
            error_msg = f"Error extracting data from {file_path}: {str(e)}"
            self.logger.error(error_msg)
            
            # Move file to error directory
            self._move_file_to_error(file_path)
            
            raise ExtractionError(error_msg)
    
    def archive_file(self, file_path: Path) -> Path:
        """
        Move processed file to archive directory.
        
        Args:
            file_path: Path to file
            
        Returns:
            Path to archived file
        """
        try:
            # Create timestamp for archive filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_filename = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            archive_path = self.archive_dir / archive_filename
            
            # Move the file
            shutil.move(str(file_path), str(archive_path))
            self.logger.info(f"Archived {file_path} to {archive_path}")
            
            return archive_path
            
        except Exception as e:
            self.logger.error(f"Failed to archive {file_path}: {str(e)}")
            raise ExtractionError(f"Failed to archive {file_path}: {str(e)}")
    
    def _move_file_to_error(self, file_path: Path) -> Path:
        """
        Move failed file to error directory.
        
        Args:
            file_path: Path to file
            
        Returns:
            Path to file in error directory
        """
        try:
            # Create timestamp for error filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            error_filename = f"{file_path.stem}_error_{timestamp}{file_path.suffix}"
            error_path = self.error_dir / error_filename
            
            # Move the file
            shutil.move(str(file_path), str(error_path))
            self.logger.info(f"Moved failed file {file_path} to {error_path}")
            
            return error_path
            
        except Exception as e:
            self.logger.error(f"Failed to move {file_path} to error directory: {str(e)}")
            return file_path