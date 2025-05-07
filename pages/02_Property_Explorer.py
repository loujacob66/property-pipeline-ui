import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from utils.database import get_db_connection, get_filtered_listings, get_all_listings
from utils.data_processing import enrich_dataframe, format_currency, format_percentage
import io
import pydeck as pdk
import subprocess
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from pathlib import Path
import sqlite3
from datetime import datetime
import sys

st.set_page_config(page_title="Property Explorer", page_icon="🔍", layout="wide")
st.title("Property Explorer")

# Get paths from session state
db_path = st.session_state.get('db_path', "")
scripts_path = st.session_state.get('default_scripts_path', "../property-pipeline/scripts")

# Define the path to the show_listing_history.py script for property history
show_history_script_path = Path(scripts_path) / "show_listing_history.py"

# Debug output for script path
# st.write(f"scripts_path: {scripts_path}") # Commented out for cleaner UI
# show_history_script_path = Path(scripts_path) / "show_listing_history.py"
# st.write(f"show_history_script_path: {show_history_script_path}  Exists: {show_history_script_path.exists()}") # Commented out

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

def get_blacklisted_addresses():
    """Get all blacklisted addresses from the database."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("""
            SELECT address, reason, blacklisted_at
            FROM address_blacklist
            ORDER BY blacklisted_at DESC
        """, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching blacklisted addresses: {e}")
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
        initial_df = get_all_listings(db_path)
        if initial_df.empty:
            st.sidebar.warning("No data available for filtering.")
            available_cities = []
        else:
            initial_df = enrich_dataframe(initial_df) # Enrich once here
            if 'city' in initial_df.columns and not initial_df['city'].isna().all():
                cities = initial_df['city'].unique()
                available_cities = sorted([str(city) for city in cities if pd.notna(city) and str(city).strip()])
            else:
                available_cities = []
except Exception as e:
    st.sidebar.error(f"Error loading filter data: {e}")
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
        st.write(f"Showing {len(df)} properties initially. Apply filters to refine results.")
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
        tab1, tab2, tab3, tab_lookup, tab4, tab5 = st.tabs(["Table View", "Map View", "Visual Analysis", "Quick Lookup", "Property History", "Blacklist Management"])
        
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
                'days_on_compass',
                'address', 
                'city',
                'price', 
                'beds', 
                'baths', 
                'sqft', 
                'price_per_sqft',
                'mls_type', 
                'walk_score', 
                'estimated_rent', 
                'rent_yield', 
                'tax_information', 
                'status',
                'url'
            ]
            # Ensure only existing columns are selected, maintaining the defined order
            columns_for_df = [col for col in columns_to_display_in_order if col in display_df.columns]

            # Configure column display settings
            column_config = {
                'days_on_compass': st.column_config.NumberColumn(
                    'Age',
                    format="%d",
                    width=60
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

            st.dataframe(
                display_df[columns_for_df], # Use the selected and ordered columns
                column_config=column_config,
                use_container_width=True,
                hide_index=True
            )

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

        with tab_lookup:
            st.header("Quick Property Lookup")
            st.write("Paste a full or partial street address below to find a property and get quick links.")
            lookup_address = st.text_input("Find Property by Address", key="lookup_address")
            if st.button("Find Property", key="find_property_button"):
                if lookup_address.strip():
                    # Case-insensitive, substring match
                    matches = df[df['address'].str.contains(lookup_address.strip(), case=False, na=False)] if 'address' in df.columns else pd.DataFrame()
                    if not matches.empty:
                        match = matches.iloc[0]
                        st.success(f"Found: {match['address']}")
                        # Compass link
                        if 'url' in match and pd.notna(match['url']) and match['url']:
                            st.markdown(f"[Compass]({match['url']})", unsafe_allow_html=True)
                        # WalkScore link
                        if 'walkscore_shorturl' in match and pd.notna(match['walkscore_shorturl']) and match['walkscore_shorturl']:
                            st.markdown(f"[WalkScore]({match['walkscore_shorturl']})", unsafe_allow_html=True)
                    else:
                        st.warning("No property found matching that address.")
                else:
                    st.info("Please enter an address to search.")

        with tab4:
            st.header("Property History")
            st.write("Paste a full street address below to view its change history. Example: '123 Main Street' (not just the street number or city).")
            manual_address = st.text_input("Street Address")
            if st.button("Show History for Address"):
                if manual_address.strip():
                    st.session_state['manual_history_output'] = show_history(manual_address.strip())
                    st.session_state['manual_history_address'] = manual_address.strip()
                else:
                    st.session_state['manual_history_output'] = ''
                    st.session_state['manual_history_address'] = ''
            if st.session_state.get('manual_history_output'):
                st.markdown(f"### Listing History for: {st.session_state['manual_history_address']}")
                st.code(st.session_state['manual_history_output'])

        with tab5:
            st.header("Blacklist Management")
            
            # Show current blacklist
            st.subheader("Current Blacklist")
            blacklist_df = get_blacklisted_addresses()
            if not blacklist_df.empty:
                st.dataframe(blacklist_df, use_container_width=True)
            else:
                st.info("No addresses are currently blacklisted.")
            
            # Add new address to blacklist
            st.subheader("Add to Blacklist")
            col1, col2 = st.columns(2)
            with col1:
                new_address = st.text_input("Address to blacklist")
            with col2:
                blacklist_reason = st.text_input("Reason for blacklisting")
            dry_run = st.checkbox("Dry Run (do not actually modify the database)")
            
            if st.button("Add to Blacklist"):
                if new_address:
                    result = manage_blacklist(new_address, blacklist_reason, dry_run=dry_run)
                    st.code(result)
                    # Refresh the blacklist display
                    st.rerun()
                else:
                    st.warning("Please enter an address to blacklist.")
            
            # Remove from blacklist
            st.subheader("Remove from Blacklist")
            if not blacklist_df.empty:
                address_to_remove = st.selectbox(
                    "Select address to remove from blacklist",
                    blacklist_df['address'].tolist()
                )
                if st.button("Remove from Blacklist"):
                    result = manage_blacklist(address_to_remove, remove=True)
                    st.code(result)
                    # Refresh the blacklist display
                    st.rerun()
            else:
                st.info("No addresses available to remove from blacklist.")

except Exception as e:
    st.error(f"Error: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
