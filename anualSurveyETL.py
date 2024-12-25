import os
import pandas as pd
import pymysql
from pymysql import Error

def get_db_connection():
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            port=3307,
            password='',
            database='etl'
        )
        if connection.open:
            print("Database connection successful")
            return connection
    except Error as e:
        print(f"Database connection failed: {e}")
        raise

def extract(file_path):
    try:
        df = pd.read_csv(file_path)
        print("Data extracted successfully")
        return df
    except Exception as e:
        print(f"Data extraction failed: {e}")
        raise

def transform(df):
    print("Columns in the dataset:")
    for column in df.columns:
        missing_count = df[column].isnull().sum()
        print(f"{column} has {missing_count} missing values")
    
    df['Value'] = df['Value'].fillna(0)
    df['Year'] = df['Year'].astype(int)
    print("Data transformation completed successfully")
    return df

def load(df):
    connection = get_db_connection()
    if connection is None:
        print('Database connection failed')
        return
    
    cursor = connection.cursor()
    
    try:
        cursor.execute("DROP TABLE IF EXISTS AnnualSurvey")
        create_table_query = """
        CREATE TABLE AnnualSurvey (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Year INT,
            Industry_aggregation_NZSIOC VARCHAR(255),
            Industry_code_NZSIOC VARCHAR(255),
            Industry_name_NZSIOC VARCHAR(255),
            Units VARCHAR(255),
            Variable_code VARCHAR(255),
            Variable_name VARCHAR(255),
            Variable_category VARCHAR(255),
            Value FLOAT,
            Industry_code_ANZSIC06 VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)

        insert_query = """
        INSERT INTO AnnualSurvey (
            Year, Industry_aggregation_NZSIOC, Industry_code_NZSIOC,
            Industry_name_NZSIOC, Units, Variable_code, Variable_name,
            Variable_category, Value, Industry_code_ANZSIC06
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, df.values.tolist())
        connection.commit()
        print("Data loaded successfully")
    except Error as e:
        print(f"Error during database operations: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def etl_process(file_path):
    try:
        print("Starting ETL process...")
        data = extract(file_path)
        transformed_data = transform(data)
        load(transformed_data)
        print("ETL process completed")
    except Exception as e:
        print(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    etl_process("annual_survey.csv")
