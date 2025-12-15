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
GROUP BY Stage;
"""
dropoff_df = run_query(dropoff_query, "Customer Journey Drop-off Points")

# 2. Average Duration per Stage
duration_query = """
SELECT Stage, ROUND(AVG(Duration), 2) AS AvgDuration
FROM customer_journey
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
GROUP BY p.ProductName
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

# 6. User Engagement by Content Type
engagement_query = """
SELECT 
    LOWER(ContentType) AS ContentType,
    ROUND(AVG(CAST(SUBSTRING_INDEX(ViewsClicksCombined, '-', 1) AS UNSIGNED)), 2) AS AvgViews,
    ROUND(AVG(CAST(SUBSTRING_INDEX(ViewsClicksCombined, '-', -1) AS UNSIGNED)), 2) AS AvgClicks,
    ROUND(AVG(Likes), 2) AS AvgLikes
FROM engagement_data
GROUP BY LOWER(ContentType)
ORDER BY AvgClicks DESC;
"""
engagement_df = run_query(engagement_query, "Engagement Rate by Content Type")


# # 7. Conversion Rate from Engagement to Purchase
# conversion_query = """
# SELECT 
#     COUNT(DISTINCT e.CustomerID) AS EngagedUsers,
#     (SELECT COUNT(DISTINCT j.CustomerID)
#      FROM customer_journey j
#      WHERE j.Action = 'Purchase') AS Buyers,
#     ROUND(
#         (SELECT COUNT(DISTINCT j.CustomerID)
#          FROM customer_journey j
#          WHERE j.Action = 'Purchase') * 100.0 / COUNT(DISTINCT e.CustomerID), 2
#     ) AS ConversionRate
# FROM engagement_data e;
# """
# conversion_df = run_query(conversion_query, "Engagement to Purchase Conversion Rate")

# 8. Top Performing Products by Engagement
top_engaged_products_query = """
SELECT 
    p.ProductName,
    COUNT(e.EngagementID) AS EngagementCount
FROM engagement_data e
JOIN products p ON e.ProductID = p.ProductID
GROUP BY p.ProductName
ORDER BY EngagementCount DESC;
"""
top_engaged_df = run_query(top_engaged_products_query, "Top Engaged Products")


cursor.close()
db_connection.close()