import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from utils.database import get_db_connection, get_filtered_listings, get_all_listings, get_blacklisted_addresses, toggle_favorite, get_favorites
from utils.data_processing import enrich_dataframe, format_currency, format_percentage
import io
import pydeck as pdk
import subprocess
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from pathlib import Path
import sqlite3
from datetime import datetime
import sys
import json
from utils.table_config import get_column_config, get_property_explorer_columns, get_map_view_columns
from utils.table_styles import get_table_styles

# Handle refresh after successful operations
if st.session_state.get('needs_refresh', False):
    st.session_state.pop('needs_refresh', None)
    st.rerun()

# Configuration
st.set_page_config(page_title="Property Explorer", page_icon="🔍", layout="wide")
st.title("Property Explorer")

# Get paths from session state
db_path = st.session_state.get('db_path', "")
scripts_path = st.session_state.get('default_scripts_path', "../property-pipeline/scripts")

# Debug output for paths
# st.write(f"[DEBUG] Database path: {db_path}")
# st.write(f"[DEBUG] Database exists: {Path(db_path).exists() if db_path else False}")

# Define the path to the show_listing_history.py script for property history
show_history_script_path = Path(scripts_path) / "show_listing_history.py"

# Debug output for script path
# st.write(f"scripts_path: {scripts_path}") # Commented out for cleaner UI
# show_history_script_path = Path(scripts_path) / "show_listing_history.py"
# st.write(f"show_history_script_path: {show_history_script_path}  Exists: {show_history_script_path.exists()}") # Commented out

def manage_blacklist(address, reason=None, remove=False, dry_run=False):
    """Add or remove an address from the blacklist using the blacklist_address.py script."""
    blacklist_script_path = Path(scripts_path) / "blacklist_address.py"
    
    if not blacklist_script_path.exists():
        return f"Error: Blacklist script not found at {blacklist_script_path}"
    
    try:
        cmd = [sys.executable, str(blacklist_script_path), "--address", address]
        if reason:
            cmd.extend(["--reason", reason])
        if remove:
            cmd.append("--remove")
        if dry_run:
            cmd.append("--dry-run")
            
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return result.stdout
    except Exception as e:
        return f"Error running blacklist script: {e}"

def show_history(address):
    """Call the show_listing_history.py script with the given address and return its output."""
    if not show_history_script_path.exists():
        return f"Script not found: {show_history_script_path}"
    try:
        result = subprocess.run(
            [sys.executable, str(show_history_script_path), address],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Script error (exit {result.returncode}):\n{result.stderr}\n{result.stdout}"
    except Exception as e:
        return f"Error running script: {e}"

def get_rental_history(listing_id):
    """Get rental history for a specific listing_id."""
    try:
        conn = sqlite3.connect(db_path)
        df_history = pd.read_sql_query("""
            SELECT date, rent
            FROM rental_history
            WHERE listing_id = ?
            ORDER BY date ASC
        """, conn, params=(listing_id,))
        conn.close()
        # Ensure date is treated as datetime for plotting and resampling
        df_history['date'] = pd.to_datetime(df_history['date'])
        return df_history
    except Exception as e:
        st.error(f"Error fetching rental history: {e}")
        return pd.DataFrame()

def get_listing_changes(listing_id):
    """Get change history for a specific listing_id."""
    try:
        conn = sqlite3.connect(db_path)
        df_changes = pd.read_sql_query("""
            SELECT field_name, old_value, new_value, changed_at, source
            FROM listing_changes
            WHERE listing_id = ?
            ORDER BY changed_at DESC
        """, conn, params=(listing_id,))
        conn.close()
        # Ensure changed_at is treated as datetime
        df_changes['changed_at'] = pd.to_datetime(df_changes['changed_at'])
        return df_changes
    except Exception as e:
        st.error(f"Error fetching listing changes: {e}")
        return pd.DataFrame()

# --- Filter Section ---
st.sidebar.header("Filters")

# Load initial data to get filter options
try:
    # Ensure db_path is valid before attempting to load
    if not db_path or not Path(db_path).exists():
         st.sidebar.error(f"Database not found at: {db_path}. Please configure it on the main page.")
         available_cities = []
         initial_df = pd.DataFrame() # Ensure initial_df exists but is empty
    else:
        # st.write("[DEBUG] Loading data from database...")
        initial_df = get_all_listings(db_path)
        # st.write(f"[DEBUG] Loaded {len(initial_df)} properties")
        if initial_df.empty:
            st.sidebar.warning("No data available for filtering.")
            available_cities = []
        else:
            # st.write("[DEBUG] Enriching data...")
            initial_df = enrich_dataframe(initial_df) # Enrich once here
            # st.write(f"[DEBUG] Data enriched. Columns: {initial_df.columns.tolist()}")
            if 'city' in initial_df.columns and not initial_df['city'].isna().all():
                cities = initial_df['city'].unique()
                available_cities = sorted([str(city) for city in cities if pd.notna(city) and str(city).strip()])
                # st.write(f"[DEBUG] Found {len(available_cities)} cities")
            else:
                available_cities = []
                # st.write("[DEBUG] No cities found in data")
except Exception as e:
    st.sidebar.error(f"Error loading filter data: {e}")
    # st.write(f"[DEBUG] Error details: {str(e)}")
    available_cities = []
    initial_df = pd.DataFrame() # Ensure initial_df exists but is empty


# City filter (multiselect) - Only filter if cities are available
if available_cities:
    selected_cities = st.sidebar.multiselect(
        "Filter by City",
        options=["All"] + available_cities,
        default=["All"],
        key="city_filter" # Added a key for stability
    )
else:
    st.sidebar.info("No cities found in the data to filter by.")
    selected_cities = ["All"] # Default to 'All' if no cities

# Apply filters button
apply_filters = st.sidebar.button("Apply Filters", key="apply_filters_button")


# Main content area
try:
    # Initialize DataFrame - Use the initially loaded df
    if 'initial_df' in locals() and not initial_df.empty:
        df = initial_df.copy() # Start with the full dataset loaded for filters
        st.write(f"Showing {len(df)} properties initially. Apply filters to refine results. Select items to add to favorites tab.")
    else:
         # If initial_df is empty (due to error or no data), df should also be empty
         df = pd.DataFrame()
         st.warning("No properties found in the database or an error occurred loading data.")


    # Apply filters if the button is pressed and we have data
    if apply_filters and not df.empty:
        filtered_df = df.copy() # Start filtering from the full dataset

        # City filter logic
        if selected_cities and "All" not in selected_cities:
            if 'city' in filtered_df.columns:
                 filtered_df = filtered_df[filtered_df['city'].isin(selected_cities)]
                 st.success(f"Filters applied. Found {len(filtered_df)} properties in selected cities.")
            else:
                 st.warning("City column not found for filtering.")
                 # Keep filtered_df as is if city column is missing
        else:
            # If 'All' is selected or no cities selected, show all (no filtering needed)
             st.success(f"Showing all {len(filtered_df)} properties.")


        df = filtered_df # Update the main df with the filtered results


    if df.empty:
        st.warning("No properties found matching your criteria.")
    else:
        # Add tabs for different views
        tab1, tab2, tab3, tab_rental_histories, tab_lookup, tab4, tab_cashflow, tab_favorites = st.tabs([
            "Table View", "Map View", "Visual Analysis", "Rental Histories", 
            "Quick Lookup", "Property History", "Cashflow & Appreciation", "Favorites"
        ])
        
        # Add custom CSS for compact, scrollable table
        st.markdown("""
            <style>
            .scroll-table-container { overflow-x: auto; }
            .compact-table th, .compact-table td { padding: 2px 6px !important; font-size: 13px !important; line-height: 1.2 !important; white-space: nowrap !important; }
            .col-address { min-width: 250px; max-width: 600px; }
            .col-bd, .col-ba { width: 24px; min-width: 24px; max-width: 32px; text-align: center; }
            .col-history { width: 60px; min-width: 60px; text-align: center; }
            </style>
        """, unsafe_allow_html=True)

        with tab1:
        
            display_df = df.copy()
            
            # Add favorite column - keep as int64 (1/0) for database compatibility
            display_df['favorite'] = display_df['favorite'].fillna(0).astype('int64')
            
            # Convert timestamps to local timezone
            if 'last_updated' in display_df.columns:
                display_df['last_updated'] = pd.to_datetime(display_df['last_updated'], format='mixed', errors='coerce')
                display_df['last_updated'] = display_df['last_updated'].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
            if 'db_updated_at' in display_df.columns:
                display_df['db_updated_at'] = pd.to_datetime(display_df['db_updated_at'], errors='coerce')
            if 'imported_at' in display_df.columns:
                display_df['imported_at'] = pd.to_datetime(display_df['imported_at'], errors='coerce')
            if 'created_at' in display_df.columns:
                display_df['created_at'] = pd.to_datetime(display_df['created_at'], errors='coerce')
            
            if 'price' in display_df.columns:
                display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'price_per_sqft' in display_df.columns:
                display_df['price_per_sqft'] = display_df['price_per_sqft'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'estimated_rent' in display_df.columns:
                display_df['estimated_rent'] = display_df['estimated_rent'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'rent_yield' in display_df.columns:
                # Convert to percentage value for NumberColumn (e.g., 0.075 -> 7.5)
                display_df['rent_yield'] = display_df['rent_yield'].apply(lambda x: x * 100 if pd.notna(x) and isinstance(x, (int, float)) else x)
            if 'walk_score' in display_df.columns:
                display_df['walk_score'] = display_df['walk_score'].apply(lambda x: f"{int(x)}" if pd.notna(x) and isinstance(x, (int, float)) else x)

            # Define the order and selection of columns to display
            columns_to_display_in_order = [
                'id',  # Always include id (hidden)
                'favorite',  # Add favorite column first
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
                'mls_type', 
                'walk_score', 
                'estimated_rent', 
                'estimated_monthly_cashflow',
                'rent_yield', 
                'tax_information', 
                'url'
            ]
            columns_for_df = [col for col in columns_to_display_in_order if col in display_df.columns]

            # Configure column display settings (do NOT include 'id')
            column_config = {
                'favorite': st.column_config.CheckboxColumn(
                    'Favorite',
                    help="Add to favorites",
                    width=40,
                    default=False,
                    required=True
                ),
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
                    width=125
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
                'status': st.column_config.TextColumn(
                    'Status',
                    width='small'
                ),
                'url': st.column_config.LinkColumn(
                    'Compass',
                    width='medium'
                )
            }

            # Filter column config to only include columns that exist in the dataframe
            column_config = {k: v for k, v in column_config.items() if k in display_df.columns}

            # Display the dataframe with editing enabled
            edited_df = st.data_editor(
                display_df[columns_for_df],
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                key="property_table"
            )

            # Handle favorite changes
            if 'property_table' in st.session_state:
                # Get the current state of favorites
                current_favorites = edited_df['favorite'].to_dict()
                previous_favorites = display_df['favorite'].to_dict()
                
                # Only process changes if there are any
                if current_favorites != previous_favorites:
                    for idx, row in edited_df.iterrows():
                        if row['favorite'] != display_df.iloc[idx]['favorite']:
                            listing_id_from_df = display_df.iloc[idx]['id']
                            listing_id_for_db = int(listing_id_from_df)  # Explicit cast to Python int
                            
                            try:
                                # Store the current state before attempting to change
                                current_state = bool(row['favorite'])
                                
                                # Attempt to update the database
                                success = toggle_favorite(db_path, listing_id_for_db, current_state)
                                
                                if success:
                                    # Update was successful, force a refresh
                                    st.session_state['needs_refresh'] = True
                                    # Clear the property_table state to force a fresh load
                                    if 'property_table' in st.session_state:
                                        del st.session_state['property_table']
                                    st.rerun()
                                else:
                                    # Update failed, reset the UI state
                                    edited_df.at[idx, 'favorite'] = display_df.iloc[idx]['favorite']
                                    st.error(f"Failed to update favorite status for {display_df.iloc[idx]['address']}")
                                    # Force a refresh to ensure UI is in sync
                                    st.session_state['needs_refresh'] = True
                                    st.rerun()
                            except Exception as e:
                                # Handle any unexpected errors
                                st.error(f"Error updating favorite: {str(e)}")
                                # Reset the favorite state to match the database
                                edited_df.at[idx, 'favorite'] = display_df.iloc[idx]['favorite']
                                # Force a refresh to ensure UI is in sync
                                st.session_state['needs_refresh'] = True
                                st.rerun()

            # Add a refresh button
            if st.button("Refresh Page to Show Updated Data"):
                st.session_state['needs_refresh'] = True
                st.rerun()

            # Dropdown to select an address
            selected_address = st.selectbox(
                "Select a property to view history:",
                display_df['address'].unique() if not display_df.empty else [] # Handle empty df
            )

            if st.button("Show History"):
                st.session_state['history_output'] = show_history(selected_address)
                st.session_state['history_address'] = selected_address
                st.session_state['active_tab'] = 'History'

            # Show history panel as before
            if st.session_state.get('active_tab') == 'History' and st.session_state['history_output']:
                st.markdown(f"### Listing History for: {st.session_state['history_address']}")
                st.code(st.session_state['history_output'])
                if st.button("Back to Table View"):
                    st.session_state['active_tab'] = 'Table View'
                    st.session_state['history_output'] = ''
                    st.session_state['history_address'] = ''

        with tab2:
            # Map view (if coordinates are available)
            st.write("### Property Map")
            
            # Add diagnostic information
            st.write("Debug Information:")
            st.write(f"Total properties in dataset: {len(df)}")
            if 'latitude' in df.columns and 'longitude' in df.columns:
                st.write(f"Properties with non-null latitude: {df['latitude'].notna().sum()}")
                st.write(f"Properties with non-null longitude: {df['longitude'].notna().sum()}")
                st.write(f"Properties with non-zero coordinates: {((df['latitude'] != 0) & (df['longitude'] != 0)).sum()}")
                
                # Show sample of coordinates
                st.write("Sample coordinates from first 5 properties:")
                sample_coords = df[['latitude', 'longitude']].head()
                st.write(sample_coords)
            
            # Prepare map data
            map_df = df[
                df['latitude'].notna() & df['longitude'].notna() &
                (df['latitude'] != 0) & (df['longitude'] != 0)
            ].copy()
            
            if not map_df.empty:
                # Convert coordinates to numeric and drop any remaining invalid entries
                map_df['lat'] = pd.to_numeric(map_df['latitude'], errors='coerce')
                map_df['lon'] = pd.to_numeric(map_df['longitude'], errors='coerce')
                map_df = map_df.dropna(subset=['lat', 'lon'])
                
                # Calculate center point for the map
                center_lat = map_df['lat'].mean()
                center_lon = map_df['lon'].mean()
                
                # Create tooltip with more property information
                tooltip = {
                    "html": """
                        <b>{address}</b><br/>
                        Price: ${price:,.0f}<br/>
                        Beds: {beds} | Baths: {baths}<br/>
                        Sqft: {sqft:,.0f}
                    """,
                    "style": {
                        "backgroundColor": "white",
                        "color": "black",
                        "padding": "10px",
                        "borderRadius": "5px",
                        "boxShadow": "0 2px 4px rgba(0,0,0,0.2)"
                    }
                }

                # Display map using pydeck
                st.pydeck_chart(pdk.Deck(
                    map_style='mapbox://styles/mapbox/light-v10',
                    initial_view_state=pdk.ViewState(
                        latitude=center_lat,
                        longitude=center_lon,
                        zoom=11,
                        pitch=0,
                    ),
                    layers=[
                        pdk.Layer(
                            'ScatterplotLayer',
                            data=map_df,
                            get_position='[lon, lat]',
                            get_color='[200, 30, 0, 160]',
                            get_radius=100,
                            pickable=True,
                            auto_highlight=True,
                            highlight_color=[255, 255, 0, 200],
                        ),
                    ],
                    tooltip=tooltip
                ))
                
                # Clickable table below
                st.write("### Properties (clickable links)")
                # Create display columns for Compass and WalkScore
                map_df['Compass'] = map_df['url'].apply(lambda x: f"<a href='{x}' target='_blank'>Compass</a>" if pd.notna(x) and x else "")
                map_df['WalkScore'] = map_df['walkscore_shorturl'].apply(lambda x: f"<a href='{x}' target='_blank'>WalkScore</a>" if pd.notna(x) and x else "")
                
                # Format the display table
                display_columns = ['address', 'price', 'beds', 'baths', 'sqft', 'Compass', 'WalkScore']
                display_df = map_df[display_columns].copy()
                display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
                
                st.write(
                    display_df.to_html(render_links=True, escape=False, index=False),
                    unsafe_allow_html=True
                )
            else:
                st.warning("No valid coordinates available to display on the map.")
        
        with tab3:
            st.write("### Visual Analysis")
            
            # Metrics to analyze
            analysis_metric = st.selectbox(
                "Select Metric to Analyze",
                ["Price", "Price per Sqft", "Rent Yield", "WalkScore"]
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if analysis_metric == "Price":
                    if 'price' in df.columns and df['price'].notna().any():
                        fig = px.histogram(
                            df, 
                            x="price", 
                            title="Price Distribution",
                            color_discrete_sequence=['#3366CC']
                        )
                        fig.update_layout(xaxis_title="Price ($)", yaxis_title="Number of Properties")
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "Price per Sqft":
                    if 'price_per_sqft' in df.columns and df['price_per_sqft'].notna().any():
                        fig = px.histogram(
                            df, 
                            x="price_per_sqft", 
                            title="Price per Sqft Distribution",
                            color_discrete_sequence=['#33CC99']
                        )
                        fig.update_layout(xaxis_title="Price per Sqft ($)", yaxis_title="Number of Properties")
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "Rent Yield":
                    if 'rent_yield' in df.columns and df['rent_yield'].notna().any():
                        fig = px.histogram(
                            df, 
                            x="rent_yield", 
                            title="Rent Yield Distribution",
                            color_discrete_sequence=['#CC6633']
                        )
                        fig.update_layout(
                            xaxis_title="Rent Yield (Annual %)", 
                            yaxis_title="Number of Properties",
                            xaxis=dict(tickformat=".0%")
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "WalkScore":
                    if 'walk_score' in df.columns and df['walk_score'].notna().any():
                        fig = px.histogram(
                            df, 
                            x="walk_score", 
                            title="WalkScore Distribution",
                            color_discrete_sequence=['#9933CC']
                        )
                        fig.update_layout(xaxis_title="WalkScore", yaxis_title="Number of Properties")
                        st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Scatter plot based on selected metric
                if analysis_metric == "Price":
                    if all(x in df.columns for x in ['price', 'sqft']) and df['price'].notna().any() and df['sqft'].notna().any():
                        fig = px.scatter(
                            df, 
                            x="sqft", 
                            y="price", 
                            title="Price vs. Square Footage",
                            color_discrete_sequence=['#3366CC'],
                            hover_data=['address', 'beds', 'baths']
                        )
                        fig.update_layout(xaxis_title="Square Footage", yaxis_title="Price ($)")
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "Price per Sqft":
                    if all(x in df.columns for x in ['price_per_sqft', 'sqft']) and df['price_per_sqft'].notna().any():
                        fig = px.scatter(
                            df, 
                            x="sqft", 
                            y="price_per_sqft", 
                            title="Price per Sqft vs. Square Footage",
                            color_discrete_sequence=['#33CC99'],
                            hover_data=['address', 'beds', 'baths']
                        )
                        fig.update_layout(xaxis_title="Square Footage", yaxis_title="Price per Sqft ($)")
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "Rent Yield":
                    if all(x in df.columns for x in ['rent_yield', 'price']) and df['rent_yield'].notna().any():
                        fig = px.scatter(
                            df, 
                            x="price", 
                            y="rent_yield", 
                            title="Rent Yield vs. Price",
                            color_discrete_sequence=['#CC6633'],
                            hover_data=['address', 'beds', 'baths', 'estimated_rent']
                        )
                        fig.update_layout(
                            xaxis_title="Price ($)", 
                            yaxis_title="Rent Yield (Annual %)",
                            yaxis=dict(tickformat=".0%")
                        )
                        st.plotly_chart(fig, use_container_width=True)
                
                elif analysis_metric == "WalkScore":
                    if all(x in df.columns for x in ['walk_score', 'price']) and df['walk_score'].notna().any():
                        fig = px.scatter(
                            df, 
                            x="walk_score", 
                            y="price", 
                            title="WalkScore vs. Price",
                            color_discrete_sequence=['#9933CC'],
                            hover_data=['address', 'city', 'beds', 'baths']
                        )
                        fig.update_layout(xaxis_title="WalkScore", yaxis_title="Price ($)")
                        st.plotly_chart(fig, use_container_width=True)
            
            # Additional analysis
            if analysis_metric == "Price" and 'city' in df.columns and df['city'].notna().any():
                # Box plot of price by city
                city_counts = df['city'].value_counts()
                top_cities = city_counts[city_counts >= 3].index.tolist()
                
                if top_cities:
                    city_df = df[df['city'].isin(top_cities)]
                    
                    fig = px.box(
                        city_df, 
                        x="city", 
                        y="price", 
                        title="Price Distribution by City",
                        color="city",
                        points="all"
                    )
                    fig.update_layout(xaxis_title="City", yaxis_title="Price ($)")
                    st.plotly_chart(fig, use_container_width=True)

        with tab_rental_histories:
            st.header("Rental Histories Over Time by Zip Code")

            if df.empty:
                st.warning("No properties to display rental histories for. Apply filters or ensure data is loaded.")
            else:
                if 'id' not in df.columns or 'zip' not in df.columns:
                    st.error("The 'id' (listing_id) or 'zip' column is missing from the property data. Cannot fetch/aggregate rental histories by zip.")
                else:
                    all_histories_list = []
                    # Ensure 'zip' is treated as string to handle variations like '94107' and '94107-1234'
                    properties_to_fetch = df[df['id'].notna() & df['zip'].notna()].copy()
                    properties_to_fetch['zip'] = properties_to_fetch['zip'].astype(str)

                    for i, row in properties_to_fetch.iterrows():
                        listing_id = int(row['id'])
                        zip_code = row['zip']
                        
                        history_df_single = get_rental_history(listing_id)
                        if not history_df_single.empty:
                            history_df_single['zip'] = zip_code
                            all_histories_list.append(history_df_single)

                    if not all_histories_list:
                        st.info("No rental history data found for the properties in the current filter.")
                    else:
                        combined_history_df = pd.concat(all_histories_list, ignore_index=True)
                        
                        if combined_history_df.empty:
                            st.info("No rental history data found after processing.")
                        else:
                            combined_history_df['date'] = pd.to_datetime(combined_history_df['date'])
                            combined_history_df = combined_history_df.sort_values(by=['zip', 'date'])

                            # Aggregate to get mean rent per zip per month
                            # Ensure 'zip' is string for consistent grouping
                            combined_history_df['zip'] = combined_history_df['zip'].astype(str)
                            monthly_avg_rent_by_zip = combined_history_df.groupby(
                                ['zip', pd.Grouper(key='date', freq='MS')] # MS for Month Start
                            )['rent'].mean().reset_index()
                            monthly_avg_rent_by_zip = monthly_avg_rent_by_zip.rename(columns={'rent': 'average_rent'})

                            if monthly_avg_rent_by_zip.empty:
                                st.info("No aggregated monthly rental data to display.")
                            else:
                                st.write("### Average Monthly Rent Trends by Zip Code")
                                
                                unique_zips = sorted(monthly_avg_rent_by_zip['zip'].unique())

                                if len(unique_zips) > 10:
                                    default_selection = unique_zips[:5] if len(unique_zips) >= 5 else unique_zips
                                    selected_zips_for_chart = st.multiselect(
                                        "Select zip codes to display on chart (max 10 recommended for readability):",
                                        options=unique_zips,
                                        default=default_selection
                                    )
                                    if not selected_zips_for_chart:
                                        st.warning("Please select at least one zip code to display.")
                                        chart_df_agg = pd.DataFrame()
                                    else:
                                        chart_df_agg = monthly_avg_rent_by_zip[monthly_avg_rent_by_zip['zip'].isin(selected_zips_for_chart)]
                                else:
                                    chart_df_agg = monthly_avg_rent_by_zip

                                if not chart_df_agg.empty:
                                    fig = px.line(
                                        chart_df_agg,
                                        x='date',
                                        y='average_rent',
                                        color='zip',
                                        title="Average Monthly Rent Over Time by Zip Code",
                                        labels={'average_rent': 'Average Monthly Rent ($)', 'date': 'Date', 'zip': 'Zip Code'},
                                        markers=True
                                    )
                                    fig.update_layout(
                                        yaxis_tickformat="$,.0f",
                                        legend_title_text='Zip Code'
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                elif len(unique_zips) > 10 and not selected_zips_for_chart:
                                    pass # Warning already shown
                                else:
                                    st.info("No rental history data to chart for the current selection of zip codes or filters.")

        with tab_lookup:
            st.header("Quick Property Lookup")
            st.write("Paste a full or partial street address below to find a property and get quick links.")
            lookup_address = st.text_input("Find Property by Address", key="lookup_address")
            
            # Use session state to store lookup results and history
            if 'lookup_result' not in st.session_state:
                st.session_state['lookup_result'] = None
            if 'rental_history_data' not in st.session_state:
                st.session_state['rental_history_data'] = pd.DataFrame()
            if 'lookup_address_display' not in st.session_state:
                 st.session_state['lookup_address_display'] = ""

            if st.button("Find Property", key="find_property_button"):
                if lookup_address.strip():
                    # Case-insensitive, substring match
                    matches = df[df['address'].str.contains(lookup_address.strip(), case=False, na=False)] if 'address' in df.columns else pd.DataFrame()
                    if not matches.empty:
                        match = matches.iloc[0]
                        st.session_state['lookup_result'] = match
                        st.session_state['lookup_address_display'] = match['address'] # Store for display
                        st.success(f"Found: {match['address']}")
                        # Compass link
                        if 'url' in match and pd.notna(match['url']) and match['url']:
                            st.markdown(f"[Compass]({match['url']})", unsafe_allow_html=True)
                        # WalkScore link
                        if 'walkscore_shorturl' in match and pd.notna(match['walkscore_shorturl']) and match['walkscore_shorturl']:
                            st.markdown(f"[WalkScore]({match['walkscore_shorturl']})", unsafe_allow_html=True)

                        # Fetch rental history if listing_id exists
                        if 'id' in match and pd.notna(match['id']):
                            st.session_state['rental_history_data'] = get_rental_history(int(match['id']))
                        else:
                            st.session_state['rental_history_data'] = pd.DataFrame()
                            st.info("Listing ID not found for this property, cannot fetch rental history.")

                    else:
                        st.session_state['lookup_result'] = None
                        st.session_state['rental_history_data'] = pd.DataFrame()
                        st.session_state['lookup_address_display'] = ""
                        st.warning("No property found matching that address.")
                else:
                    st.session_state['lookup_result'] = None
                    st.session_state['rental_history_data'] = pd.DataFrame()
                    st.session_state['lookup_address_display'] = ""
                    st.info("Please enter an address to search.")

            # Display rental history if available after lookup
            if st.session_state['lookup_result'] is not None and not st.session_state['rental_history_data'].empty:
                st.subheader(f"Rental History for {st.session_state['lookup_address_display']}")

                history_df = st.session_state['rental_history_data'].copy()

                # --- Quarterly Table ---
                st.write("#### Quarterly Rental History")
                # Resample to get the last rent recorded in each quarter
                quarterly_history = history_df.set_index('date').resample('QS').agg(
                    last_rent=('rent', 'last') # Get the last rent of the quarter
                ).dropna(subset=['last_rent']).reset_index()
                
                # Format the quarter start date
                quarterly_history['Quarter'] = quarterly_history['date'].dt.to_period('Q').astype(str)
                quarterly_history['Rent'] = quarterly_history['last_rent'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")

                if not quarterly_history.empty:
                     st.dataframe(
                        quarterly_history[['Quarter', 'Rent']],
                        use_container_width=True,
                        hide_index=True
                     )
                else:
                    st.info("No quarterly history available.")


                # --- Monthly Line Chart ---
                st.write("#### Monthly Rent Trend")
                if not history_df.empty:
                    fig = px.line(
                        history_df,
                        x="date",
                        y="rent",
                        title="Monthly Rent Over Time"
                    )
                    fig.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Monthly Rent ($)",
                        yaxis=dict(tickformat="$,.0f") # Format y-axis as currency
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                     st.info("No monthly history available for charting.")

            elif st.session_state['lookup_result'] is not None and st.session_state['rental_history_data'].empty and st.session_state.get('lookup_address_display'):
                 # Only show this if a property was found but had no history
                 st.info(f"No rental history found for {st.session_state['lookup_address_display']}.")


        with tab4:
            st.header("Property History")
            st.write("Paste a full street address below to view its change history. Example: '123 Main Street' (not just the street number or city).")
            manual_address = st.text_input("Street Address")
            if st.button("Show History for Address"):
                if manual_address.strip():
                    # First get the listing_id for the address
                    try:
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT id FROM listings WHERE address = ?", (manual_address.strip(),))
                        result = cursor.fetchone()
                        conn.close()
                        
                        if result:
                            listing_id = result[0]
                            # Get both listing changes and rental history
                            changes_df = get_listing_changes(listing_id)
                            rental_df = get_rental_history(listing_id)
                            
                            st.markdown(f"### Listing History for: {manual_address.strip()}")
                            
                            # Display listing changes
                            if not changes_df.empty:
                                st.subheader("Property Changes")
                                # Format the changes for display
                                changes_df['changed_at'] = changes_df['changed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                                st.dataframe(
                                    changes_df,
                                    column_config={
                                        'changed_at': st.column_config.TextColumn('Date'),
                                        'field_name': st.column_config.TextColumn('Field'),
                                        'old_value': st.column_config.TextColumn('Old Value'),
                                        'new_value': st.column_config.TextColumn('New Value'),
                                        'source': st.column_config.TextColumn('Source')
                                    },
                                    use_container_width=True,
                                    hide_index=True
                                )
                            else:
                                st.info("No property changes found in history.")
                            
                            # Display rental history
                            if not rental_df.empty:
                                st.subheader("Rental History")
                                # Format the rental history for display
                                rental_df['date'] = rental_df['date'].dt.strftime('%Y-%m-%d')
                                rental_df['rent'] = rental_df['rent'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")
                                st.dataframe(
                                    rental_df,
                                    column_config={
                                        'date': st.column_config.TextColumn('Date'),
                                        'rent': st.column_config.TextColumn('Rent')
                                    },
                                    use_container_width=True,
                                    hide_index=True
                                )
                            else:
                                st.info("No rental history found.")
                        else:
                            st.warning(f"No property found with address: {manual_address.strip()}")
                    except Exception as e:
                        st.error(f"Error fetching property history: {e}")
                else:
                    st.warning("Please enter an address to view history.")

        with tab_cashflow:
            st.header("Cashflow & Appreciation Analysis")
            st.write("Analyze a property's potential cashflow and appreciation over time.")

            # Load config file for defaults
            config_path = Path(scripts_path).parent / "config" / "cashflow_config.json"
            try:
                with open(config_path, 'r') as f:
                    cashflow_config = json.load(f)
            except Exception as e:
                st.warning(f"Could not load config file: {config_path}\n{e}")
                cashflow_config = {}

            # Property selection
            selected_address = st.selectbox(
                "Select a property to analyze:",
                display_df['address'].unique() if not display_df.empty else []
            )

            if selected_address:
                property_data = display_df[display_df['address'] == selected_address].iloc[0]
                property_data_raw = df[df['address'] == selected_address].iloc[0]  # Get the original, unformatted row
                import re
                def parse_currency(val):
                    if isinstance(val, str):
                        digits = re.sub(r'[^\d.]', '', val)
                        try:
                            return float(digits)
                        except Exception:
                            return 0.0
                    try:
                        return float(val)
                    except Exception:
                        return 0.0

                # Helper to get default: config > property > fallback
                def get_default(key, prop_key=None, fallback=0.0):
                    v = cashflow_config.get(key, None)
                    if v is not None:
                        try:
                            return float(v)
                        except Exception:
                            return v
                    if prop_key and prop_key in property_data:
                        return parse_currency(property_data[prop_key])
                    return fallback

                with st.form("cashflow_analysis_form"):
                    st.subheader("Analysis Parameters")
                    col1, col2 = st.columns(2)
                    with col1:
                        down_payment = st.number_input(
                            "Down Payment ($)",
                            min_value=0.0,
                            value=get_default('down_payment', 'price', 0.0),
                            step=10000.0
                        )
                        interest_rate = st.number_input(
                            "Interest Rate (%)",
                            min_value=0.0,
                            max_value=20.0,
                            value=get_default('rate', None, 7.0),
                            step=0.1
                        )
                        insurance = st.number_input(
                            "Annual Insurance ($)",
                            min_value=0.0,
                            value=get_default('insurance', None, 2000.0),
                            step=100.0
                        )
                        misc_monthly = st.number_input(
                            "Misc Monthly Costs ($)",
                            min_value=0.0,
                            value=get_default('misc_monthly', None, 100.0),
                            step=10.0
                        )
                        # Add rent field
                        rent = st.number_input(
                            "Monthly Rent ($)",
                            min_value=0.0,
                            value=parse_currency(property_data_raw.get('estimated_rent', 0.0)),
                            step=50.0
                        )
                    with col2:
                        loan_term = st.number_input(
                            "Loan Term (Years)",
                            min_value=1,
                            max_value=30,
                            value=int(get_default('loan_term', None, 30)),
                            step=1
                        )
                        vacancy_rate = st.number_input(
                            "Vacancy Rate (%)",
                            min_value=0.0,
                            max_value=100.0,
                            value=get_default('vacancy_rate', None, 5.0),
                            step=0.5
                        )
                        property_mgmt_fee = st.number_input(
                            "Property Management Fee (%)",
                            min_value=0.0,
                            max_value=20.0,
                            value=get_default('property_mgmt_fee', None, 8.0),
                            step=0.5
                        )
                        investment_horizon = st.number_input(
                            "Investment Horizon (Years)",
                            min_value=1,
                            max_value=30,
                            value=int(get_default('investment_horizon', None, 5)),
                            step=1
                        )
                    with st.expander("Advanced Options"):
                        col3, col4 = st.columns(2)
                        with col3:
                            maintenance_percent = st.number_input(
                                "Annual Maintenance (% of value)",
                                min_value=0.0,
                                max_value=10.0,
                                value=get_default('maintenance_percent', None, 1.0),
                                step=0.1
                            )
                            capex_percent = st.number_input(
                                "Annual CapEx Reserve (% of value)",
                                min_value=0.0,
                                max_value=10.0,
                                value=get_default('capex_percent', None, 1.0),
                                step=0.1
                            )
                            utilities_monthly = st.number_input(
                                "Monthly Utilities (Landlord)",
                                min_value=0.0,
                                value=get_default('utilities_monthly', None, 0.0),
                                step=10.0
                            )
                        with col4:
                            property_age = st.number_input(
                                "Property Age (Years)",
                                min_value=0,
                                value=int(get_default('property_age', None, 20)),
                                step=1
                            )
                            property_condition = st.selectbox(
                                "Property Condition",
                                options=["excellent", "good", "fair", "poor"],
                                index=["excellent", "good", "fair", "poor"].index(str(cashflow_config.get('property_condition', 'good')))
                                if cashflow_config.get('property_condition') in ["excellent", "good", "fair", "poor"] else 1
                            )
                            use_dynamic_capex = st.checkbox("Use Dynamic CapEx Calculations", value=bool(cashflow_config.get('use_dynamic_capex', True)))
                    submitted = st.form_submit_button("Run Analysis")

                if submitted:
                    # Prepare command arguments
                    cmd = [
                        sys.executable,
                        str(Path(scripts_path) / "appreciation_and_cashflow_analyzer.py"),
                        "--address", selected_address,
                        "--down-payment", str(down_payment),
                        "--rate", str(interest_rate),
                        "--insurance", str(insurance),
                        "--misc-monthly", str(misc_monthly),
                        "--loan-term", str(loan_term),
                        "--vacancy-rate", str(vacancy_rate),
                        "--property-mgmt-fee", str(property_mgmt_fee),
                        "--investment-horizon", str(investment_horizon),
                        "--maintenance-percent", str(maintenance_percent),
                        "--capex-percent", str(capex_percent),
                        "--utilities-monthly", str(utilities_monthly),
                        "--property-age", str(property_age),
                        "--property-condition", property_condition
                    ]

                    if rent > 0:
                        cmd.extend(["--rent", str(rent)])

                    if use_dynamic_capex:
                        cmd.append("--use-dynamic-capex")

                    # Run the analysis
                    try:
                        result = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=60
                        )
                        
                        if result.returncode == 0:
                            st.success("Analysis completed successfully!")
                            st.code(result.stdout)
                        else:
                            st.error(f"Error running analysis: {result.stderr}")
                    except Exception as e:
                        st.error(f"Error running analysis: {str(e)}")

        with tab_favorites:
            st.header("Favorite Properties")
            
            try:
                # Get favorite properties using the same pattern as Data Enrichment
                conn = get_db_connection(db_path)
                favorites_df = get_favorites(db_path)
                
                # Debug: Show raw favorites DataFrame
                # st.subheader("[Debug] Raw favorites DataFrame")
                # st.write(favorites_df)
                
                if favorites_df.empty:
                    st.info("No favorite properties yet. Use the checkboxes in the Table View to add properties to your favorites.")
                else:
                    # Format the display
                    display_favorites = favorites_df.copy()
                    
                    # Convert timestamps
                    if 'last_updated' in display_favorites.columns:
                        display_favorites['last_updated'] = pd.to_datetime(display_favorites['last_updated'], format='mixed', errors='coerce')
                        display_favorites['last_updated'] = display_favorites['last_updated'].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
                    if 'db_updated_at' in display_favorites.columns:
                        display_favorites['db_updated_at'] = pd.to_datetime(display_favorites['db_updated_at'], errors='coerce')
                    if 'imported_at' in display_favorites.columns:
                        display_favorites['imported_at'] = pd.to_datetime(display_favorites['imported_at'], errors='coerce')
                    if 'created_at' in display_favorites.columns:
                        display_favorites['created_at'] = pd.to_datetime(display_favorites['created_at'], errors='coerce')
                    
                    # Format currency columns
                    if 'price' in display_favorites.columns:
                        display_favorites['price'] = display_favorites['price'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
                    if 'estimated_rent' in display_favorites.columns:
                        display_favorites['estimated_rent'] = display_favorites['estimated_rent'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
                    if 'estimated_monthly_cashflow' in display_favorites.columns:
                        display_favorites['estimated_monthly_cashflow'] = display_favorites['estimated_monthly_cashflow'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) and isinstance(x, (int, float)) else x)
                    if 'rent_yield' in display_favorites.columns:
                        # Convert to percentage value (e.g., 0.075 -> 7.5) for NumberColumn formatting
                        display_favorites['rent_yield'] = display_favorites['rent_yield'].apply(lambda x: x * 100 if pd.notna(x) and isinstance(x, (int, float)) else x)
                    
                    # Select columns to display
                    columns_to_display = [
                        'last_updated',
                        'address',
                        'city',
                        'price',
                        'status',
                        'beds',
                        'baths',
                        'rent_yield',
                        'url'
                    ]
                    columns_to_display = [col for col in columns_to_display if col in display_favorites.columns]
                    
                    # Configure column display
                    column_config = {
                        'last_updated': st.column_config.DatetimeColumn(
                            'Last Update',
                            format="MM/DD/YY",
                            width=90
                        ),
                        'address': st.column_config.TextColumn(
                            'Address',
                            width=175
                        ),
                        'city': st.column_config.TextColumn(
                            'City',
                            width=110
                        ),
                        'price': st.column_config.TextColumn(
                            'Price',
                            width=100
                        ),
                        'status': st.column_config.TextColumn(
                            'Status',
                            width=80
                        ),
                        'beds': st.column_config.NumberColumn(
                            'Beds',
                            width=40
                        ),
                        'baths': st.column_config.NumberColumn(
                            'Baths',
                            width=50
                        ),
                        'rent_yield': st.column_config.NumberColumn(
                            'Rent Yield',
                            format="%.1f%%",
                            width=80
                        ),
                        'url': st.column_config.LinkColumn(
                            'Compass',
                            width=100
                        )
                    }
                    
                    # For display only, drop 'id' column
                    display_favorites_for_table = display_favorites[columns_to_display].copy()
                    if 'id' in display_favorites_for_table.columns:
                        display_favorites_for_table = display_favorites_for_table.drop(columns=['id'])
                    st.dataframe(
                        display_favorites_for_table,
                        column_config=column_config,
                        use_container_width=True,
                        hide_index=True
                    )

                    # Add a refresh button
                    if st.button("Refresh Favorites", key="refresh_favorites"):
                        st.session_state['needs_refresh'] = True
                        st.rerun()

            except Exception as e:
                st.error(f"Error loading favorites: {e}")
                st.info(f"Make sure the database exists at: {db_path} and the schema is as expected.")

except Exception as e:
    st.error(f"Error: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
