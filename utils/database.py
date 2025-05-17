import sqlite3
import pandas as pd

def get_db_connection(db_path):
    """Connect to the SQLite database."""
    return sqlite3.connect(db_path)

def get_all_listings(db_path, limit=None):
    """Get all property listings from the database."""
    conn = get_db_connection(db_path)
    # Explicitly list columns based on provided schema
    columns = [
        "id", "address", "city", "state", "zip", "price", "beds", "baths", "sqft",
        "price_per_sqft", "url", "from_collection", "source", "imported_at",
        "estimated_rent", "rent_yield", "mls_number", "mls_type", "tax_information",
        "days_on_compass", "last_updated", "favorite", "year_built", "lot_size",
        "hoa_fee", "parking", "heating", "cooling", "style", "construction",
        "days_on_market", "status", "agent_name", "agent_phone", "agent_email",
        "schools_json", "price_history_json", "walk_score", "transit_score", "bike_score",
        "walkscore_shorturl", "compass_shorturl", "latitude", "longitude",
        "estimated_monthly_cashflow",
        "created_at", "last_updated_at"
    ]
    query = f"SELECT {', '.join([f'\"{col}\"' for col in columns])} FROM listings" # Quote column names
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        # Define dtype dictionary for numeric columns
        dtype_dict = {
            'price': 'float64',
            'beds': 'float64',
            'baths': 'float64',
            'sqft': 'float64',
            'price_per_sqft': 'float64',
            'estimated_rent': 'float64',
            'rent_yield': 'float64',
            'year_built': 'float64',
            'hoa_fee': 'float64',
            'walk_score': 'float64',
            'transit_score': 'float64',
            'bike_score': 'float64',
            'latitude': 'float64',
            'longitude': 'float64',
            'estimated_monthly_cashflow': 'float64'
        }
        
        df = pd.read_sql_query(query, conn, dtype=dtype_dict)
        conn.close()
        return df
    except Exception as e:
        # Log or handle error, maybe return column names that failed
        print(f"Error executing query: {query}")
        print(f"Error: {e}")
        # Try SELECT * as a fallback for debugging? Or raise error.
        # For now, let's return an empty df if specific columns fail
        conn.close()
        return pd.DataFrame()

def get_filtered_listings(db_path, filters=None):
    """Get property listings with filters applied."""
    conn = get_db_connection(db_path)
    # Explicitly list columns based on provided schema
    columns = [
        "id", "address", "city", "state", "zip", "price", "beds", "baths", "sqft",
        "price_per_sqft", "url", "from_collection", "source", "imported_at",
        "estimated_rent", "rent_yield", "mls_number", "mls_type", "tax_information",
        "days_on_compass", "last_updated", "favorite", "year_built", "lot_size",
        "hoa_fee", "parking", "heating", "cooling", "style", "construction",
        "days_on_market", "status", "agent_name", "agent_phone", "agent_email",
        "schools_json", "price_history_json", "walk_score", "transit_score", "bike_score",
        "walkscore_shorturl", "compass_shorturl", "latitude", "longitude",
        "estimated_monthly_cashflow",
        "created_at", "last_updated_at"
    ]
    # Quote column names in SELECT statement
    query = f"""
        SELECT {', '.join([f'\"{col}\"' for col in columns])} 
        FROM listings l
        WHERE 1=1
        AND NOT EXISTS (
            SELECT 1 FROM address_blacklist b 
            WHERE LOWER(l.address) = LOWER(b.address)
        )
    """
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
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
         print(f"Error executing query: {query}")
         print(f"Params: {params}")
         print(f"Error: {e}")
         conn.close()
         return pd.DataFrame()

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

def get_blacklisted_addresses(db_path):
    """Get all blacklisted addresses from the database."""
    conn = get_db_connection(db_path)
    try:
        df = pd.read_sql_query("""
            SELECT address, reason, added_at, added_by 
            FROM address_blacklist 
            ORDER BY added_at DESC
        """, conn)
        return df
    except Exception as e:
        print(f"Error fetching blacklisted addresses: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def is_address_blacklisted(db_path, address):
    """Check if an address is blacklisted."""
    conn = get_db_connection(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM address_blacklist 
            WHERE LOWER(address) = LOWER(?)
        """, (address,))
        return cursor.fetchone() is not None
    finally:
        conn.close()

def add_to_blacklist(db_path, address, reason=None, added_by='system'):
    """Add an address to the blacklist."""
    conn = get_db_connection(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO address_blacklist (address, reason, added_by)
            VALUES (?, ?, ?)
        """, (address, reason, added_by))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Error adding address to blacklist: {e}")
        return False
    finally:
        conn.close()

def remove_from_blacklist(db_path, address):
    """Remove an address from the blacklist."""
    conn = get_db_connection(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM address_blacklist 
            WHERE LOWER(address) = LOWER(?)
        """, (address,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Error removing address from blacklist: {e}")
        return False
    finally:
        conn.close()
