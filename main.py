import os
import sys
import argparse
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import concurrent.futures
import traceback

from config.settings import Settings
from utils.logger import setup_logger
from utils.exceptions import ETLError
from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import Loader

def process_file(
    file_path: Path,
    extractor: Extractor,
    transformer: Transformer,
    loader: Loader,
    transformations: List[Dict[str, Any]],
    validations: List[Dict[str, Any]],
    table_name: str,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Process a single CSV file through the ETL pipeline.
    
    Args:
        file_path: Path to CSV file
        extractor: Extractor instance
        transformer: Transformer instance
        loader: Loader instance
        transformations: List of transformations
        validations: List of validations
        table_name: Target table name
        schema: Target database schema (optional)
        
    Returns:
        Dictionary with processing results
    """
    logger = logging.getLogger("etl_pipeline")
    result = {
        "file": str(file_path),
        "success": False,
        "rows_processed": 0,
        "rows_loaded": 0,
        "error": None
    }
    
    try:
        logger.info(f"Starting ETL process for {file_path}")
        
        # Extract
        total_rows = 0
        total_loaded = 0
        
        for batch_idx, df in enumerate(extractor.extract_from_file(file_path)):
            batch_rows = len(df)
            total_rows += batch_rows
            logger.info(f"Processing batch {batch_idx + 1} with {batch_rows} rows")
            
            # Transform
            transformed_df = transformer.transform(df, transformations)
            
            # Validate
            if validations:
                transformer.validate_data(transformed_df, validations)
            
            # Load
            rows_loaded = loader.load_dataframe(
                transformed_df,
                table_name, 
                schema=schema,
                if_exists="append" if batch_idx > 0 or extractor.list_csv_files().index(file_path) > 0 else "replace"
            )
            
            total_loaded += rows_loaded
            logger.info(f"Loaded {rows_loaded} rows from batch {batch_idx + 1}")
        
        # Archive the file after successful processing
        extractor.archive_file(file_path)
        
        result["success"] = True
        result["rows_processed"] = total_rows
        result["rows_loaded"] = total_loaded
        
        logger.info(f"ETL process completed for {file_path}: {total_rows} rows processed, {total_loaded} rows loaded")
        
    except ETLError as e:
        error_msg = f"ETL error processing {file_path}: {str(e)}"
        logger.error(error_msg)
        result["error"] = error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error processing {file_path}: {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        result["error"] = error_msg
    
    return result

def main() -> None:
    """Main entry point for the ETL pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="CSV to Database ETL Pipeline")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--table", help="Target table name")
    parser.add_argument("--schema", help="Target database schema")
    args = parser.parse_args()
    
    try:
        # Load settings
        settings = Settings(args.config)
        
        # Set up logger
        logger = setup_logger(settings.get("logging"))
        logger.info("Starting ETL pipeline")
        
        # Initialize components
        extractor = Extractor(settings.get("csv"), logger)
        transformer = Transformer(logger)
        loader = Loader(settings.get("database"), logger)
        
        # Connect to database
        loader.connect()
        
        # Define table name
        table_name = args.table or "csv_data"
        schema = args.schema
        
        # List CSV files
        csv_files = extractor.list_csv_files()
        
        if not csv_files:
            logger.info("No CSV files found to process")
            return
            
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        # Define transformations and validations
        # These could come from a configuration file in a real-world scenario
        transformations = [
            {
                "type": "convert_types",
                "mapping": {
                    "transaction_id": "int",
                    "date": "datetime",
                    "customer_id": "str",
                    "product_id": "str",
                    "product_name": "str",
                    "quantity": "int",
                    "unit_price": "float",
                    "total_amount": "float"
                }
            },
            {
                "type": "fill_na",
                "columns": {
                    "quantity": 0,
                    "unit_price": 0.0,
                    "total_amount": 0.0
                }
            }
        ]
        
        validations = [
            {
                "type": "not_null",
                "columns": ["transaction_id", "date", "customer_id", "product_id"]
            }
        ]
        
        # Process in parallel or sequentially
        processing_config = settings.get("processing")
        parallel = processing_config.get("parallel", True)
        max_workers = processing_config.get("max_workers", 4)
        
        results = []
        
        if parallel and len(csv_files) > 1:
            logger.info(f"Processing {len(csv_files)} files in parallel with {max_workers} workers")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(
                        process_file, 
                        file, 
                        extractor, 
                        transformer, 
                        loader, 
                        transformations, 
                        validations, 
                        table_name, 
                        schema
                    ): file for file in csv_files
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file = future_to_file[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Error processing {file}: {str(e)}")
                        results.append({
                            "file": str(file),
                            "success": False,
                            "error": str(e)
                        })
        else:
            logger.info(f"Processing {len(csv_files)} files sequentially")
            
            for file in csv_files:
                result = process_file(
                    file, 
                    extractor, 
                    transformer, 
                    loader, 
                    transformations, 
                    validations, 
                    table_name, 
                    schema
                )
                results.append(result)
        
        # Summarize results
        successful = sum(1 for r in results if r["success"])
        total_rows = sum(r.get("rows_processed", 0) for r in results)
        total_loaded = sum(r.get("rows_loaded", 0) for r in results)
        
        logger.info(f"ETL pipeline completed. {successful}/{len(results)} files processed successfully.")
        logger.info(f"Total rows processed: {total_rows}, total rows loaded: {total_loaded}")
        
        if successful != len(results):
            logger.warning(f"Failed files:")
            for result in results:
                if not result["success"]:
                    logger.warning(f"  {result['file']}: {result.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        # Clean up
        if 'loader' in locals() and loader:
            loader.disconnect()

if __name__ == "__main__":
    main()