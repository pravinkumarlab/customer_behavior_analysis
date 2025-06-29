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