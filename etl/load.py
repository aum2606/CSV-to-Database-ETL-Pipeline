import logging
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.exc import SQLAlchemyError
import time
from typing import Dict, List, Any, Optional, Tuple

from utils.exceptions import LoadError, DatabaseError

class Loader:
    """Load data into a database."""
    
    def __init__(self, db_config: Dict[str, Any], logger: logging.Logger):
        """
        Initialize loader with database configuration.
        
        Args:
            db_config: Database configuration
            logger: Logger instance
        """
        self.db_config = db_config
        self.logger = logger
        self.engine = None
        self.connection = None
        self.metadata = MetaData()  # Create without bind parameter
        
        # Connection settings
        self.connection_timeout = db_config.get("connection_timeout", 30)
        self.max_retries = db_config.get("max_retries", 3)
        self.retry_delay = db_config.get("retry_delay", 5)
    
    def connect(self) -> None:
        """
        Establish database connection.
        
        Raises:
            DatabaseError: If connection fails
        """
        db_type = self.db_config["type"]
        host = self.db_config["host"]
        port = self.db_config["port"]
        database = self.db_config["database"]
        user = self.db_config["user"]
        password = self.db_config["password"]
        
        # Create connection string based on database type
        if db_type.lower() == "postgresql":
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        elif db_type.lower() == "mysql":
            conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        else:
            raise DatabaseError(f"Unsupported database type: {db_type}")
        
        # Attempt connection with retries
        retries = 0
        last_error = None
        
        while retries < self.max_retries:
            try:
                self.logger.info(f"Connecting to {db_type} database at {host}:{port}/{database}")
                self.engine = create_engine(
                    conn_str,
                    connect_args={"connect_timeout": self.connection_timeout}
                )
                self.connection = self.engine.connect()
                
                # Set engine to metadata after connection is established
                self.metadata = MetaData()
                self.metadata.bind = self.engine
                
                self.logger.info("Database connection established")
                return
                
            except Exception as e:
                last_error = str(e)
                retries += 1
                self.logger.warning(f"Connection attempt {retries} failed: {last_error}")
                
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
        
        # If we get here, all retries failed
        error_msg = f"Failed to connect to database after {self.max_retries} attempts: {last_error}"
        self.logger.error(error_msg)
        raise DatabaseError(error_msg)
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
    
    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            schema: Database schema (optional)
            
        Returns:
            True if table exists
        """
        if not self.engine:
            self.connect()
            
        inspector = inspect(self.engine)
        return inspector.has_table(table_name, schema=schema)
    
    def create_table_from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        dtypes: Optional[Dict[str, Any]] = None,
        if_exists: str = "fail",
        primary_key: Optional[List[str]] = None
    ) -> None:
        """
        Create a table based on DataFrame schema.
        
        Args:
            df: DataFrame with data schema
            table_name: Name of the table
            schema: Database schema (optional)
            dtypes: SQLAlchemy column types (optional)
            if_exists: Action if table exists ("fail", "replace", "append")
            primary_key: List of primary key columns (optional)
            
        Raises:
            DatabaseError: If table creation fails
        """
        try:
            # Handle table existence based on if_exists parameter
            if self.table_exists(table_name, schema):
                if if_exists == "fail":
                    raise DatabaseError(f"Table {table_name} already exists")
                elif if_exists == "replace":
                    self.drop_table(table_name, schema)
                # For "append", we don't need to do anything
            
            # Create table
            df.head(0).to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists="replace",
                index=False,
                dtype=dtypes
            )
            
            # Add primary key if specified
            if primary_key:
                self._add_primary_key(table_name, schema, primary_key)
                
            self.logger.info(f"Created table {schema+'.' if schema else ''}{table_name}")
            
        except Exception as e:
            error_msg = f"Error creating table {schema+'.' if schema else ''}{table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg)
    
    def _add_primary_key(self, table_name: str, schema: Optional[str] = None, primary_key: List[str] = None) -> None:
        """
        Add primary key constraint to a table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (optional)
            primary_key: List of primary key columns
            
        Raises:
            DatabaseError: If adding primary key fails
        """
        if not primary_key:
            return
            
        try:
            pk_name = f"pk_{table_name}"
            qualified_table = f"{schema+'.' if schema else ''}{table_name}"
            
            # SQL for adding primary key
            sql = f"ALTER TABLE {qualified_table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(primary_key)})"
            
            with self.engine.begin() as conn:
                conn.execute(sql)
                
            self.logger.info(f"Added primary key {primary_key} to table {qualified_table}")
            
        except Exception as e:
            error_msg = f"Error adding primary key to {schema+'.' if schema else ''}{table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg)
    
    def drop_table(self, table_name: str, schema: Optional[str] = None) -> None:
        """
        Drop a table.
        
        Args:
            table_name: Name of the table
            schema: Database schema (optional)
            
        Raises:
            DatabaseError: If dropping table fails
        """
        try:
            qualified_table = f"{schema+'.' if schema else ''}{table_name}"
            
            # SQL for dropping table
            sql = f"DROP TABLE IF EXISTS {qualified_table}"
            
            with self.engine.begin() as conn:
                conn.execute(sql)
                
            self.logger.info(f"Dropped table {qualified_table}")
            
        except Exception as e:
            error_msg = f"Error dropping table {schema+'.' if schema else ''}{table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg)
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = "append",
        chunk_size: int = 10000
    ) -> int:
        """
        Load DataFrame data into a table.
        
        Args:
            df: DataFrame with data
            table_name: Name of the table
            schema: Database schema (optional)
            if_exists: Action if table exists ("fail", "replace", "append")
            chunk_size: Batch size for loading
            
        Returns:
            Number of rows loaded
            
        Raises:
            LoadError: If loading fails
        """
        try:
            if not self.engine:
                self.connect()
                
            # Check if table exists
            if not self.table_exists(table_name, schema) and if_exists != "replace":
                self.logger.info(f"Table {schema+'.' if schema else ''}{table_name} does not exist, creating it")
                self.create_table_from_dataframe(df, table_name, schema, if_exists="replace")
            
            # Load data in chunks
            rows_loaded = 0
            qualified_table = f"{schema+'.' if schema else ''}{table_name}"
            
            self.logger.info(f"Loading {len(df)} rows into {qualified_table}")
            
            # Use to_sql for loading
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method="multi"
            )
            
            rows_loaded = len(df)
            self.logger.info(f"Loaded {rows_loaded} rows into {qualified_table}")
            
            return rows_loaded
            
        except Exception as e:
            error_msg = f"Error loading data into {schema+'.' if schema else ''}{table_name}: {str(e)}"
            self.logger.error(error_msg)
            raise LoadError(error_msg)
    
    def execute_sql(self, sql: str) -> Any:
        """
        Execute SQL statement.
        
        Args:
            sql: SQL statement
            
        Returns:
            Query result
            
        Raises:
            DatabaseError: If execution fails
        """
        try:
            if not self.connection:
                self.connect()
                
            self.logger.debug(f"Executing SQL: {sql}")
            result = self.connection.execute(sql)
            return result
            
        except Exception as e:
            error_msg = f"Error executing SQL: {str(e)}"
            self.logger.error(error_msg)
            raise DatabaseError(error_msg)