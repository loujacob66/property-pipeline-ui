import sqlite3
import pandas as pd

def get_db_connection(db_path):
    """Connect to the SQLite database."""
    return sqlite3.connect(db_path)

def get_all_listings(db_path, limit=None):
    """Get all property listings from the database."""
    conn = get_db_connection(db_path)
    query = "SELECT * FROM listings"
    if limit:
        query += f" LIMIT {limit}"
    return pd.read_sql_query(query, conn)

def get_filtered_listings(db_path, filters=None):
    """Get property listings with filters applied."""
    conn = get_db_connection(db_path)
    query = "SELECT * FROM listings WHERE 1=1"
    params = []
    
    if filters:
        for column, value in filters.items():
            if isinstance(value, tuple) and len(value) == 2:
                # Range filter (min, max)
                min_val, max_val = value
                if min_val is not None:
                    query += f" AND {column} >= ?"
                    params.append(min_val)
                if max_val is not None:
                    query += f" AND {column} <= ?"
                    params.append(max_val)
            elif isinstance(value, tuple) and value[0] == "IS NOT NULL":
                # Handle IS NOT NULL condition
                query += f" AND {column} IS NOT NULL"
            else:
                # Exact match
                query += f" AND {column} = ?"
                params.append(value)
    
    return pd.read_sql_query(query, conn, params=params)

def get_summary_stats(db_path):
    """Get summary statistics for the database."""
    conn = get_db_connection(db_path)
    stats = {}
    
    # Total count
    query = "SELECT COUNT(*) as count FROM listings"
    result = pd.read_sql_query(query, conn)
    stats['total_count'] = result['count'].iloc[0]
    
    # Average price
    query = "SELECT AVG(price) as avg_price FROM listings WHERE price IS NOT NULL"
    result = pd.read_sql_query(query, conn)
    stats['avg_price'] = result['avg_price'].iloc[0]
    
    # Average sqft
    query = "SELECT AVG(sqft) as avg_sqft FROM listings WHERE sqft IS NOT NULL"
    result = pd.read_sql_query(query, conn)
    stats['avg_sqft'] = result['avg_sqft'].iloc[0]
    
    # Average price per sqft
    query = "SELECT AVG(price_per_sqft) as avg_price_per_sqft FROM listings WHERE price_per_sqft IS NOT NULL"
    result = pd.read_sql_query(query, conn)
    stats['avg_price_per_sqft'] = result['avg_price_per_sqft'].iloc[0]
    
    # City counts
    query = "SELECT city, COUNT(*) as count FROM listings WHERE city IS NOT NULL GROUP BY city ORDER BY count DESC LIMIT 10"
    stats['city_counts'] = pd.read_sql_query(query, conn)
    
    # Average price by city
    query = """
        SELECT city, AVG(price) as avg_price 
        FROM listings 
        WHERE city IS NOT NULL AND price IS NOT NULL 
        GROUP BY city 
        ORDER BY avg_price DESC 
        LIMIT 10
    """
    stats['city_prices'] = pd.read_sql_query(query, conn)
    
    # MLS type distribution
    query = """
        SELECT mls_type, COUNT(*) as count 
        FROM listings 
        WHERE mls_type IS NOT NULL 
        GROUP BY mls_type 
        ORDER BY count DESC
    """
    stats['mls_types'] = pd.read_sql_query(query, conn)
    
    return stats
