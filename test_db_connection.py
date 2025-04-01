import psycopg2
from sqlalchemy import create_engine

# Connection parameters - update with your values
params = {
    "host": "localhost",
    "port": "5432",
    "database": "etl_demo",
    "user": "etl_user",
    "password": "etl_password"
}

def test_psycopg2_connection():
    """Test connection using psycopg2 driver"""
    print("Testing connection with psycopg2...")
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=params["host"],
            port=params["port"],
            database=params["database"],
            user=params["user"],
            password=params["password"]
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Execute a simple query
        cur.execute("SELECT version();")
        
        # Fetch and print the result
        version = cur.fetchone()
        print(f"Successfully connected to PostgreSQL: {version[0]}")
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Connection closed successfully.")
        return True
        
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return False

def test_sqlalchemy_connection():
    """Test connection using SQLAlchemy"""
    print("\nTesting connection with SQLAlchemy...")
    
    try:
        # Create connection string
        conn_str = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
        
        # Create engine
        engine = create_engine(conn_str)
        
        # Connect and execute query
        with engine.connect() as conn:
            result = conn.execute("SELECT version();")
            version = result.fetchone()
            print(f"Successfully connected to PostgreSQL using SQLAlchemy: {version[0]}")
            
        print("SQLAlchemy connection closed successfully.")
        return True
        
    except Exception as e:
        print(f"Error connecting with SQLAlchemy: {e}")
        return False

if __name__ == "__main__":
    psycopg2_success = test_psycopg2_connection()
    sqlalchemy_success = test_sqlalchemy_connection()
    
    if psycopg2_success and sqlalchemy_success:
        print("\n✅ Database connection tests passed! Your ETL pipeline should work correctly.")
    else:
        print("\n❌ Database connection tests failed. Please check your PostgreSQL setup and credentials.")