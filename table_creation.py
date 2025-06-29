import pandas as pd
import mysql.connector as db

# Connect to MySQL server (no database specified)
db_connection = db.connect(
    host = "localhost",
    user = "root",
    password = "root"
)
cursor = db_connection.cursor()

# Create the database
schema_name = "customer_behaviour_analysis"
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
cursor.close()
db_connection.close()


# reconnect, now specifying the database
db_connection = db.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = schema_name
)
cursor = db_connection.cursor()

# define a table creation script for all csv
table_creation_scripts = {
    "geography" : """ 
        CREATE TABLE IF NOT EXISTS geography (
            geographyid INT PRIMARY KEY,
            country VARCHAR(100),
            city VARCHAR(100)
        );
    """,
    "products" : """
        CREATE TABLE IF NOT EXISTS products(
            productid INT PRIMARY KEY,
            productname VARCHAR(255),
            category VARCHAR(100),
            price DECIMAL(10,2)
        );
    """,
    "customers" : """
        CREATE TABLE IF NOT EXISTS customers(
            customerid INT PRIMARY KEY,
            customername VARCHAR(255),
            email VARCHAR(255),
            gender VARCHAR(10),
            age INT,
            geographyid INT,
            FOREIGN KEY (geographyid) REFERENCES geography(geographyid)
        );
    """,
    "customer_journey" : """
        CREATE TABLE IF NOT EXISTS customer_journey(
            journeyid INT PRIMARY KEY,
            customerid INT,
            productid INT,
            visitdate DATE,
            stage VARCHAR(50),
            action VARCHAR(50),
            duration FLOAT,
            FOREIGN KEY (customerid) REFERENCES customers(customerid),
            FOREIGN KEY (productid) REFERENCES products(productid)
        );
    """,
    "customer_reviews" : """
        CREATE TABLE IF NOT EXISTS customer_reviews(
            reviewid INT PRIMARY KEY,
            customerid INT,
            productid INT,
            reviewdate DATE,
            rating INT,
            reviewtext TEXT,
            FOREIGN KEY (customerid) REFERENCES customers(customerid),
            FOREIGN KEY (productid) REFERENCES products(productid)
        );
    """,
    "engagement_data" : """
        CREATE TABLE IF NOT EXISTS engagement_data(
            engagementid INT PRIMARY KEY,
            contentid INT,
            contenttype VARCHAR(50),
            likes INT,
            engagementdate DATE,
            campaignid INT,
            productid INT,
            viewsclickscombined VARCHAR(20),
            FOREIGN KEY (productid) REFERENCES products(productid)
        );
    """
}

# Table creation
for table_sql in table_creation_scripts.values():
    for action in table_sql.strip().split(";"):
        if action.strip():
            cursor.execute(action)

cursor.close()
db_connection.close()
