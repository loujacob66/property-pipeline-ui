import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from utils.database import get_db_connection, get_all_listings, get_summary_stats
from utils.data_processing import enrich_dataframe, format_currency, format_percentage

# Configuration
st.set_page_config(
    page_title="Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define paths
if 'base_path' not in st.session_state:
    st.session_state['base_path'] = Path(__file__).parent.parent
    # Default paths
    st.session_state['default_db_path'] = "../property-pipeline/data/listings.db"
    st.session_state['default_scripts_path'] = "../property-pipeline/scripts"
    st.session_state['default_config_path'] = "../property-pipeline/config"

# Sidebar for configuration
with st.sidebar:
    st.title("Configuration")
    # Database path
    db_path = st.text_input(
        "Database Path", 
        value=str(st.session_state['default_db_path']),
        help="Path to your listings.db SQLite database"
    )
    st.session_state['db_path'] = db_path
    # Quick links
    st.subheader("Quick Links")
    st.write("Navigate to:")
    st.page_link("pages/02_Property_Explorer.py", label="🔍 Property Explorer", icon="🔍")
    st.page_link("pages/03_Data_Enrichment.py", label="🔄 Data Enrichment", icon="🔄")
    st.page_link("pages/04_Analytics.py", label="📈 Analytics", icon="📈")

# Main content
st.title("Property Pipeline Dashboard")
st.write("""
Welcome to the Property Pipeline Dashboard. This application helps you visualize and manage your real estate property data.
Use the sidebar to navigate between different views.
""")

# Check database connection
try:
    # Try to connect to the database
    conn = get_db_connection(db_path)
    df = get_all_listings(db_path, limit=1000)  # Increased limit to show more data
    conn.close()
    
    if df.empty:
        st.warning("Database connected, but no data found. Use the Data Enrichment page to populate your database.")
    else:
        # Enrich the dataframe with calculated fields
        df = enrich_dataframe(df)
        
        # Display some key metrics
        st.header("Quick Stats")
        
        try:
            stats = get_summary_stats(db_path)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Properties", f"{stats['total_count']:,}")
            
            with col2:
                avg_price = stats['avg_price']
                if avg_price:
                    st.metric("Average Price", f"${avg_price:,.0f}")
                else:
                    st.metric("Average Price", "N/A")
            
            with col3:
                avg_sqft = stats['avg_sqft']
                if avg_sqft:
                    st.metric("Average Sqft", f"{avg_sqft:,.0f}")
                else:
                    st.metric("Average Sqft", "N/A")
            
            with col4:
                avg_price_per_sqft = stats['avg_price_per_sqft']
                if avg_price_per_sqft:
                    st.metric("Avg Price/Sqft", f"${avg_price_per_sqft:,.0f}")
                else:
                    st.metric("Avg Price/Sqft", "N/A")
            
            # Recent listings with enhanced table (matching Property Explorer)
            st.header("Recent Updates")
            st.write("Main Property Table:")
            display_df = df.copy()

            # Convert timestamps to local timezone
            if 'last_updated' in display_df.columns:
                display_df['last_updated'] = pd.to_datetime(display_df['last_updated'], format='mixed', errors='coerce')
                display_df['last_updated'] = display_df['last_updated'].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')

            if 'price' in display_df.columns:
                display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'price_per_sqft' in display_df.columns:
                display_df['price_per_sqft'] = display_df['price_per_sqft'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'estimated_rent' in display_df.columns:
                display_df['estimated_rent'] = display_df['estimated_rent'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'rent_yield' in display_df.columns:
                display_df['rent_yield'] = display_df['rent_yield'].apply(lambda x: x * 100 if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'walk_score' in display_df.columns:
                display_df['walk_score'] = display_df['walk_score'].apply(lambda x: f"{int(x)}" if pd.notna(x) and isinstance(x, (int, float)) else x)

            # Define the order and selection of columns to display (matching Property Explorer)
            columns_to_display_in_order = [
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
            columns_for_df = [col for col in columns_to_display_in_order if col in display_df.columns]

            # Configure column display settings
            column_config = {
                'last_updated': st.column_config.DatetimeColumn(
                    'Last Update',
                    format="MM/DD/YY",
                    width=90
                ),
                'db_updated_at': st.column_config.DatetimeColumn(
                    'DB Update',
                    format="MM/DD/YY",
                    width=90
                ),
                'days_on_compass': st.column_config.NumberColumn(
                    'Age',
                    format="%d",
                    width=40
                ),
                'address': st.column_config.TextColumn(
                    'Address',
                    width=175
                ),
                'city': st.column_config.TextColumn(
                    'City',
                    width=110
                ),
                'price': st.column_config.NumberColumn(
                    'Price',
                    format="$%d",
                    width='small'
                ),
                 'status': st.column_config.TextColumn(
                    'Status',
                    width='small'
                ),
                'beds': st.column_config.NumberColumn(
                    'Beds',
                    width=40
                ),
                'baths': st.column_config.NumberColumn(
                    'Baths',
                    width=50
                ),
                'sqft': st.column_config.NumberColumn(
                    'Sq Ft',
                    format="%d",
                    width=60
                ),
                'price_per_sqft': st.column_config.NumberColumn(
                    '$/SQFT',
                    format="$%d",
                    width=60
                ),
                'mls_type': st.column_config.TextColumn(
                    'MLS Type',
                    width='small'
                ),
                'walk_score': st.column_config.NumberColumn(
                    'Walk Score',
                    width='small'
                ),
                'estimated_rent': st.column_config.NumberColumn(
                    'Est. Rent',
                    format="$%d",
                    width='small'
                ),
                'estimated_monthly_cashflow': st.column_config.NumberColumn(
                    'Cashflow',
                    format="$%d",
                    width='small'
                ),
                'rent_yield': st.column_config.NumberColumn(
                    'Rent Yield',
                    format="%.1f%%",
                    width='small'
                ),
                'tax_information': st.column_config.TextColumn(
                    'Tax Details',
                    width='small'
                ),
                'url': st.column_config.LinkColumn(
                    'Compass',
                    width='medium'
                )
            }
            column_config = {k: v for k, v in column_config.items() if k in display_df.columns}

            st.dataframe(
                display_df[columns_for_df].sort_values('last_updated', ascending=False),
                column_config=column_config,
                use_container_width=True,
                hide_index=True
            )
            
            # Link to explore more
            st.write("Use the Property Explorer page to see more listings and apply filters.")
            st.page_link("pages/02_Property_Explorer.py", label="Go to Property Explorer", icon="🔍")
            
        except Exception as e:
            st.error(f"Error getting statistics: {e}")
            
except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
    
    # Show init button
    if st.button("Initialize Database"):
        from utils.script_runner import run_init_db
        
        init_script_path = Path(st.session_state['default_scripts_path']) / "init_db.py"
        if init_script_path.exists():
            with st.spinner("Initializing database..."):
                result = run_init_db(init_script_path)
                
                if result['returncode'] == 0:
                    st.success("Database initialized successfully!")
                    st.experimental_rerun()
                else:
                    st.error("Failed to initialize database")
                    st.text_area("Error", result['stderr'], height=200)
        else:
            st.error(f"Init script not found at: {init_script_path}")
