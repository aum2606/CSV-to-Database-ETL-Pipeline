class ETLError(Exception):
    """Base class for all ETL pipeline exceptions"""
    pass

class ConfigurationError(ETLError):
    """Exception raised for configuration errors"""
    pass

class ExtractionError(ETLError):
    """Exception raised for extraction errors"""
    pass

class ValidationError(ETLError):
    """Exception raised for data validation errors"""
    pass

class TransformationError(ETLError):
    """Exception raised for errors during data transformation"""
    pass

class LoadError(ETLError):
    """Exception raised for errors during data loading"""
    pass

class DatabaseError(ETLError):
    """Exception raised for database-related errors"""
    pass


