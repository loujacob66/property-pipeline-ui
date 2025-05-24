import streamlit as st

# Standard column widths
COLUMN_WIDTHS = {
    # Fixed width columns
    'checkbox': 40,  # For selection checkboxes
    'date': 90,      # For date columns
    'age': 40,       # For age/days columns
    'beds': 40,      # For bedroom count
    'baths': 50,     # For bathroom count
    'sqft': 60,      # For square footage
    'price': 100,    # For price columns
    'score': 80,     # For scores (walk score, etc.)
    'percentage': 80,# For percentage columns
    
    # Variable width columns
    'address': 175,  # For address
    'city': 110,     # For city names
    'status': 80,    # For status
    'tax_info': 120, # For tax information
    'link': 100,     # For URL links
    'mls': 100,      # For MLS numbers
}

def get_column_config(interactive=False):
    """Get standardized column configuration for property tables"""
    config = {
        'last_updated': st.column_config.DatetimeColumn(
            'Last Update',
            format="MM/DD/YY",
            width=COLUMN_WIDTHS['date']
        ),
        'db_updated_at': st.column_config.DatetimeColumn(
            'DB Update',
            format="MM/DD/YY",
            width=COLUMN_WIDTHS['date']
        ),
        'days_on_compass': st.column_config.NumberColumn(
            'Age',
            format="%d",
            width=COLUMN_WIDTHS['age']
        ),
        'address': st.column_config.TextColumn(
            'Address',
            width=COLUMN_WIDTHS['address']
        ),
        'city': st.column_config.TextColumn(
            'City',
            width=COLUMN_WIDTHS['city']
        ),
        'price': st.column_config.NumberColumn(
            'Price',
            format="$%d",
            width=COLUMN_WIDTHS['price']
        ),
        'status': st.column_config.TextColumn(
            'Status',
            width=COLUMN_WIDTHS['status']
        ),
        'beds': st.column_config.NumberColumn(
            'Beds',
            width=COLUMN_WIDTHS['beds']
        ),
        'baths': st.column_config.NumberColumn(
            'Baths',
            width=COLUMN_WIDTHS['baths']
        ),
        'sqft': st.column_config.NumberColumn(
            'Sq Ft',
            format="%d",
            width=COLUMN_WIDTHS['sqft']
        ),
        'price_per_sqft': st.column_config.NumberColumn(
            '$/SQFT',
            format="$%d",
            width=COLUMN_WIDTHS['price']
        ),
        'mls_number': st.column_config.TextColumn(
            'MLS Number',
            width=COLUMN_WIDTHS['mls']
        ),
        'mls_type': st.column_config.TextColumn(
            'MLS Type',
            width=COLUMN_WIDTHS['mls']
        ),
        'walk_score': st.column_config.NumberColumn(
            'Walk Score',
            width=COLUMN_WIDTHS['score']
        ),
        'estimated_rent': st.column_config.NumberColumn(
            'Est. Rent',
            format="$%d",
            width=COLUMN_WIDTHS['price']
        ),
        'estimated_monthly_cashflow': st.column_config.NumberColumn(
            'Cashflow',
            format="$%d",
            width=COLUMN_WIDTHS['price']
        ),
        'rent_yield': st.column_config.NumberColumn(
            'Rent Yield',
            format="%.1f%%",
            width=COLUMN_WIDTHS['percentage']
        ),
        'tax_information': st.column_config.TextColumn(
            'Tax Info',
            width=COLUMN_WIDTHS['tax_info']
        ),
        'url': st.column_config.LinkColumn(
            'Compass',
            width=COLUMN_WIDTHS['link']
        )
    }
    
    if interactive:
        config['selected'] = st.column_config.CheckboxColumn(
            'Select',
            help="Select properties to process",
            default=False,
            width=COLUMN_WIDTHS['checkbox']
        )
    
    return config

# Column sets for different pages/tabs
def get_compass_enrichment_columns():
    """Columns to display in Compass Enrichment tab"""
    return [
        'selected',
        'address',
        'db_updated_at',
        'city',
        'days_on_compass',
        'price',
        'beds',
        'baths',
        'sqft',
        'mls_number',
        'mls_type',
        'tax_information'
    ]

def get_property_explorer_columns():
    """Columns to display in Property Explorer main table"""
    return [
        'last_updated',
        'db_updated_at',
        'days_on_compass',
        'address',
        'city',
        'price',
        'status',
        'beds',
        'baths',
        'sqft',
        'price_per_sqft',
        'mls_type',
        'walk_score',
        'estimated_rent',
        'estimated_monthly_cashflow',
        'rent_yield',
        'tax_information',
        'url'
    ]

def get_map_view_columns():
    """Columns to display in Map View table"""
    return [
        'address',
        'price',
        'beds',
        'baths',
        'sqft',
        'url'
    ] 