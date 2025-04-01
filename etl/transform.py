import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Callable
import re
from datetime import datetime

from utils.exceptions import TransformationError, ValidationError

class Transformer:
    """Transform and validate data."""
    
    def __init__(self, logger: logging.Logger):
        """
        Initialize transformer.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
        self.transformation_registry = {}
    
    def register_transformation(self, name: str, transformation_fn: Callable) -> None:
        """
        Register a transformation function.
        
        Args:
            name: Transformation name
            transformation_fn: Transformation function
        """
        self.transformation_registry[name] = transformation_fn
        self.logger.debug(f"Registered transformation '{name}'")
    
    def transform(self, df: pd.DataFrame, transformations: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Apply transformations to DataFrame.
        
        Args:
            df: Input DataFrame
            transformations: List of transformation configurations
            
        Returns:
            Transformed DataFrame
        """
        transformed_df = df.copy()
        
        try:
            for transformation in transformations:
                t_type = transformation.get("type")
                
                if t_type == "rename_columns":
                    mapping = transformation.get("mapping", {})
                    transformed_df = self._rename_columns(transformed_df, mapping)
                    
                elif t_type == "drop_columns":
                    columns = transformation.get("columns", [])
                    transformed_df = self._drop_columns(transformed_df, columns)
                    
                elif t_type == "fill_na":
                    columns = transformation.get("columns", {})
                    transformed_df = self._fill_na(transformed_df, columns)
                    
                elif t_type == "convert_types":
                    type_mapping = transformation.get("mapping", {})
                    transformed_df = self._convert_types(transformed_df, type_mapping)
                    
                elif t_type == "custom":
                    name = transformation.get("name")
                    params = transformation.get("params", {})
                    if name in self.transformation_registry:
                        transformed_df = self.transformation_registry[name](transformed_df, **params)
                    else:
                        self.logger.warning(f"Custom transformation '{name}' not found")
                
                else:
                    self.logger.warning(f"Unknown transformation type: {t_type}")
            
            self.logger.info(f"Applied {len(transformations)} transformations")
            return transformed_df
            
        except Exception as e:
            error_msg = f"Error during transformation: {str(e)}"
            self.logger.error(error_msg)
            raise TransformationError(error_msg)
    
    def validate_data(self, df: pd.DataFrame, validations: List[Dict[str, Any]]) -> bool:
        """
        Validate DataFrame against rules.
        
        Args:
            df: DataFrame to validate
            validations: List of validation rules
            
        Returns:
            True if all validations pass
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            for validation in validations:
                v_type = validation.get("type")
                
                if v_type == "not_null":
                    columns = validation.get("columns", [])
                    for column in columns:
                        if df[column].isnull().any():
                            raise ValidationError(f"Column '{column}' contains NULL values")
                
                elif v_type == "unique":
                    columns = validation.get("columns", [])
                    for column in columns:
                        if df[column].duplicated().any():
                            raise ValidationError(f"Column '{column}' contains duplicate values")
                
                elif v_type == "range":
                    column = validation.get("column")
                    min_val = validation.get("min")
                    max_val = validation.get("max")
                    
                    if min_val is not None and df[column].min() < min_val:
                        raise ValidationError(f"Column '{column}' contains values below minimum {min_val}")
                        
                    if max_val is not None and df[column].max() > max_val:
                        raise ValidationError(f"Column '{column}' contains values above maximum {max_val}")
                
                elif v_type == "regex":
                    column = validation.get("column")
                    pattern = validation.get("pattern")
                    if not df[column].astype(str).str.match(pattern).all():
                        raise ValidationError(f"Column '{column}' contains values not matching pattern '{pattern}'")
                
                elif v_type == "custom":
                    validation_fn = validation.get("function")
                    if not validation_fn(df):
                        raise ValidationError(f"Custom validation failed: {validation.get('message', 'No message provided')}")
            
            self.logger.info(f"Data passed {len(validations)} validations")
            return True
            
        except ValidationError as e:
            raise e
        except Exception as e:
            error_msg = f"Error during validation: {str(e)}"
            self.logger.error(error_msg)
            raise ValidationError(error_msg)
    
    def _rename_columns(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """Rename columns based on mapping."""
        return df.rename(columns=mapping)
    
    def _drop_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Drop specified columns."""
        return df.drop(columns=[col for col in columns if col in df.columns])
    
    def _fill_na(self, df: pd.DataFrame, columns: Dict[str, Any]) -> pd.DataFrame:
        """Fill NA values in specified columns."""
        result = df.copy()
        for column, value in columns.items():
            if column in result.columns:
                result[column] = result[column].fillna(value)
        return result
    
    def _convert_types(self, df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
        """Convert column data types."""
        result = df.copy()
        
        for column, dtype in type_mapping.items():
            if column not in result.columns:
                self.logger.warning(f"Column '{column}' not found for type conversion")
                continue
                
            try:
                if dtype == "datetime":
                    result[column] = pd.to_datetime(result[column], errors='coerce')
                elif dtype == "float":
                    result[column] = pd.to_numeric(result[column], errors='coerce').astype(float)
                elif dtype == "int":
                    result[column] = pd.to_numeric(result[column], errors='coerce').astype('Int64')  # Nullable integer type
                elif dtype == "str":
                    result[column] = result[column].astype(str)
                elif dtype == "bool":
                    result[column] = result[column].astype(bool)
                else:
                    result[column] = result[column].astype(dtype)
            except Exception as e:
                self.logger.error(f"Error converting column '{column}' to type '{dtype}': {str(e)}")
                raise TransformationError(f"Error converting column '{column}' to type '{dtype}': {str(e)}")
                
        return result

    # Instance methods instead of static methods
    def standardize_text(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Standardize text in specified columns (lowercase, strip whitespace)."""
        result = df.copy()
        for column in columns:
            if column in result.columns and result[column].dtype == object:
                result[column] = result[column].astype(str).str.lower().str.strip()
        return result
    
    def add_date_parts(self, df: pd.DataFrame, date_column: str, drop_original: bool = False) -> pd.DataFrame:
        """Extract date components from a date column."""
        result = df.copy()
        
        if date_column not in result.columns:
            return result
            
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_dtype(result[date_column]):
            result[date_column] = pd.to_datetime(result[date_column], errors='coerce')
            
        # Extract date parts
        result[f"{date_column}_year"] = result[date_column].dt.year
        result[f"{date_column}_month"] = result[date_column].dt.month
        result[f"{date_column}_day"] = result[date_column].dt.day
        result[f"{date_column}_dayofweek"] = result[date_column].dt.dayofweek
        result[f"{date_column}_quarter"] = result[date_column].dt.quarter
        
        # Drop original column if requested
        if drop_original:
            result = result.drop(columns=[date_column])
            
        return result