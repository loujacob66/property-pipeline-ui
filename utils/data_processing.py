import pandas as pd
import numpy as np

def calculate_price_per_sqft(df):
    """Calculate price per square foot for listings."""
    if 'price' in df.columns and 'sqft' in df.columns:
        mask = (df['price'] > 0) & (df['sqft'] > 0)
        df.loc[mask, 'price_per_sqft'] = df.loc[mask, 'price'] / df.loc[mask, 'sqft']
    return df

def calculate_rent_yield(df):
    """Calculate annual rent yield as a percentage."""
    if all(x in df.columns for x in ['price', 'estimated_rent']):
        mask = (df['price'] > 0) & (df['estimated_rent'] > 0)
        df.loc[mask, 'rent_yield'] = (df.loc[mask, 'estimated_rent'] * 12) / df.loc[mask, 'price']
    return df

def categorize_walkscore(df):
    """Add a categorical column for WalkScore ranges."""
    if 'walk_score' in df.columns:
        conditions = [
            (df['walk_score'] < 25),
            (df['walk_score'] >= 25) & (df['walk_score'] < 50),
            (df['walk_score'] >= 50) & (df['walk_score'] < 70),
            (df['walk_score'] >= 70) & (df['walk_score'] < 90),
            (df['walk_score'] >= 90)
        ]
        choices = ['Car-Dependent', 'Car-Dependent', 'Somewhat Walkable', 'Very Walkable', 'Walker\'s Paradise']
        df['walk_score_category'] = np.select(conditions, choices, default='Unknown')
    return df

def categorize_rent_yield(df):
    """Add a categorical column for rent yield ranges."""
    if 'rent_yield' in df.columns:
        conditions = [
            (df['rent_yield'] < 0.03),
            (df['rent_yield'] >= 0.03) & (df['rent_yield'] < 0.05),
            (df['rent_yield'] >= 0.05) & (df['rent_yield'] < 0.07),
            (df['rent_yield'] >= 0.07) & (df['rent_yield'] < 0.1),
            (df['rent_yield'] >= 0.1)
        ]
        choices = ['Very Low', 'Low', 'Average', 'Good', 'Excellent']
        df['yield_category'] = np.select(conditions, choices, default='Unknown')
    return df

def categorize_price(df):
    """Add a categorical column for price ranges."""
    if 'price' in df.columns:
        bins = [0, 250000, 500000, 750000, 1000000, 1500000, 2000000, float('inf')]
        labels = ['<$250K', '$250K-$500K', '$500K-$750K', '$750K-$1M', '$1M-$1.5M', '$1.5M-$2M', '$2M+']
        df['price_category'] = pd.cut(df['price'], bins=bins, labels=labels)
    return df

def enrich_dataframe(df):
    """Apply all enrichment functions to a dataframe."""
    df = calculate_price_per_sqft(df)
    df = calculate_rent_yield(df)
    df = categorize_walkscore(df)
    df = categorize_rent_yield(df)
    df = categorize_price(df)
    return df

def format_currency(value):
    """Format a value as currency."""
    if pd.isna(value) or value is None:
        return 'N/A'
    return f"${value:,.0f}"

def format_percentage(value):
    """Format a value as percentage."""
    if pd.isna(value) or value is None:
        return 'N/A'
    return f"{value * 100:.2f}%"

def get_top_properties_by_yield(df, n=10):
    """Get top N properties by rent yield."""
    if 'rent_yield' not in df.columns:
        return pd.DataFrame()
    
    return df.sort_values('rent_yield', ascending=False).head(n)

def get_properties_needing_enrichment(df):
    """Get properties that need data enrichment."""
    # Properties missing WalkScore data
    walkscore_missing = df[df['walk_score'].isna()]
    
    # Properties missing MLS info
    mls_missing = df[df['mls_number'].isna() | df['mls_type'].isna()]
    
    # Properties missing tax info
    tax_missing = df[df['tax_information'].isna()]
    
    return {
        'walkscore_missing': walkscore_missing,
        'mls_missing': mls_missing,
        'tax_missing': tax_missing
    }
