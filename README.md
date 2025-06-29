# 📊 Customer Behavior Analysis – ShopEasy

This repository contains a Customer Behavior Analysis project for the e-commerce platform ShopEasy.
---

## 🧰 Technologies Used

- Python (pandas, mysql.connector)
- MySQL (Structured Query Language)

---

## 📁 Dataset Overview

| File                    | 
|-------------------------|
| customers.csv           |
| customer_journey.csv    |
| customer_reviews.csv    |
| products.csv            |
| engagement_data.csv     |
| geography.csv           |

---

## Step 1: Create Table in MySQL Through Python

```python
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
```
---

## Step 2: Python - Insert Data Into MySQL

```python
import pandas as pd
import mysql.connector as db


# Load all uploaded CSVs
customer_journey_df = pd.read_csv(r'E:\guvi\project\customer_behavior_analysis\data\customer_journey.csv')
customer_reviews_df = pd.read_csv(r"E:\guvi\project\customer_behavior_analysis\data\customer_reviews.csv")
customers_df = pd.read_csv(r"E:\guvi\project\customer_behavior_analysis\data\customers.csv")
engagement_data_df = pd.read_csv(r"E:\guvi\project\customer_behavior_analysis\data\engagement_data.csv")
geography_df = pd.read_csv(r"E:\guvi\project\customer_behavior_analysis\data\geography.csv")
products_df = pd.read_csv(r"E:\guvi\project\customer_behavior_analysis\data\products.csv")

# Cleaning functions
def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

def drop_duplicates_and_na(df, subset=None):
    df = df.drop_duplicates(subset=subset)
    df = df.fillna(0)
    return df

# cleaning each DataFrame
customer_journey_df = clean_column_names(customer_journey_df)
customer_reviews_df = clean_column_names(customer_reviews_df)
customers_df = clean_column_names(customers_df)
engagement_data_df = clean_column_names(engagement_data_df)
geography_df = clean_column_names(geography_df)
products_df = clean_column_names(products_df)

customer_journey_df = drop_duplicates_and_na(customer_journey_df)
customer_reviews_df = drop_duplicates_and_na(customer_reviews_df)
customers_df = drop_duplicates_and_na(customers_df)
engagement_data_df = drop_duplicates_and_na(engagement_data_df)
geography_df = drop_duplicates_and_na(geography_df)
products_df = drop_duplicates_and_na(products_df)


# Connect to customer_behaviour_analysis database
db_connection = db.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "customer_behaviour_analysis"
)
cursor = db_connection.cursor()

# Insert cleaned data into tables
def insert_dataframe(df, table):
    for i, row in df.iterrows():
        cols = ", ".join(row.index)
        placeholders = ", ".join(["%s"] * len(row))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
    db_connection.commit()

# Insert in dependency order
insert_dataframe(geography_df, "geography")
insert_dataframe(products_df, "products")
insert_dataframe(customers_df, "customers")
insert_dataframe(customer_journey_df, "customer_journey")
insert_dataframe(customer_reviews_df, "customer_reviews")
insert_dataframe(engagement_data_df, "engagement_data")

cursor.close()
db_connection.close()
```
---

## Step 3: Python - Analysis

```python
import pandas as pd
import mysql.connector as db

# Connect to customer_behaviour_analysis database
db_connection = db.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "customer_behaviour_analysis"
)
cursor = db_connection.cursor()

def run_query(query, label):
    print(f"\n--- {label} ---")
    df = pd.read_sql(query, db_connection)
    print(df.head(), "\n")
    return df

# 1. Drop-off Points in Customer Journey
dropoff_query = """
SELECT stage, COUNT(*) AS dropoffcount
FROM customer_journey
WHERE Action = 'Drop-off'
GROUP BY Stage
ORDER BY DropOffCount DESC;
"""
dropoff_df = run_query(dropoff_query, "Customer Journey Drop-off Points")

# 2. Average Duration per Stage
duration_query = """
SELECT Stage, ROUND(AVG(Duration), 2) AS AvgDuration
FROM customer_journey
WHERE Duration IS NOT NULL
GROUP BY Stage;
"""
duration_df = run_query(duration_query, "Average Duration Per Stage")

# 3. Top Rated & Lowest Rated Products
rating_query = """
SELECT 
    p.ProductName, 
    AVG(r.Rating) AS AvgRating,
    COUNT(r.ReviewID) AS ReviewCount
FROM customer_reviews r
JOIN products p ON r.ProductID = p.ProductID
GROUP BY r.ProductID
ORDER BY AvgRating DESC;
"""
rating_df = run_query(rating_query, "Product Ratings Summary")

# 4. Returning vs First-Time Buyers
repeat_buyers_query = """
SELECT 
    c.CustomerID,
    COUNT(DISTINCT j.JourneyID) AS TotalPurchases
FROM customer_journey j
JOIN customers c ON j.CustomerID = c.CustomerID
WHERE j.Action = 'Purchase'
GROUP BY c.CustomerID
ORDER BY TotalPurchases DESC;
"""
repeat_df = run_query(repeat_buyers_query, "Repeat vs First-Time Buyers")

# 5. Best Performing Products by Region
region_query = """
SELECT 
    g.Country,
    p.ProductName,
    COUNT(j.JourneyID) AS TotalPurchases
FROM customer_journey j
JOIN customers c ON j.CustomerID = c.CustomerID
JOIN geography g ON c.GeographyID = g.GeographyID
JOIN products p ON j.ProductID = p.ProductID
WHERE j.Action = 'Purchase'
GROUP BY g.Country, p.ProductName
ORDER BY g.Country, TotalPurchases DESC;
"""
region_df = run_query(region_query, "Top Products by Region")

cursor.close()
db_connection.close()
```
---

## 👏 Authors

Project done by **Pravinkumar S** | GitHub: pravinkumarlab

---
