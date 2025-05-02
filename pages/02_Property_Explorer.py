import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from utils.database import get_db_connection, get_filtered_listings, get_all_listings
from utils.data_processing import enrich_dataframe, format_currency, format_percentage

st.set_page_config(page_title="Property Explorer", page_icon="🔍", layout="wide")
st.title("Property Explorer")

# Get paths from session state
db_path = st.session_state.get('db_path', "")

# Filtering options
st.sidebar.header("Filters")

# Price range filter
price_range = st.sidebar.slider(
    "Price Range ($)", 
    min_value=0, 
    max_value=10000000, 
    value=(0, 5000000),
    step=50000,
    format="$%d"
)

# Beds and baths filters
beds_range = st.sidebar.slider(
    "Bedrooms", 
    min_value=0, 
    max_value=10, 
    value=(0, 10),
    step=1
)

baths_range = st.sidebar.slider(
    "Bathrooms", 
    min_value=0.0, 
    max_value=10.0, 
    value=(0.0, 10.0),
    step=0.5
)

# Location filters
location_expander = st.sidebar.expander("Location Filters")
with location_expander:
    city = st.text_input("City")
    state = st.text_input("State")
    zip_code = st.text_input("ZIP Code")

# Advanced filters
advanced_expander = st.sidebar.expander("Advanced Filters")
with advanced_expander:
    min_sqft = st.number_input("Min Sqft", min_value=0, step=100)
    min_rent_yield = st.number_input("Min Rent Yield (%)", min_value=0.0, step=0.1, value=0.0)
    max_price_per_sqft = st.number_input("Max Price Per Sqft ($)", min_value=0, step=10, value=1000)
    
    # MLS and Status
    mls_type_options = ["Any", "Attached", "Detached"]
    selected_mls_type = st.selectbox("MLS Type", mls_type_options)
    
    status_options = ["Any", "Active", "Pending", "Sold", "Coming Soon"]
    selected_status = st.selectbox("Status", status_options)
    
    # WalkScore
    min_walk_score = st.slider("Min WalkScore", min_value=0, max_value=100, value=0)

# Apply filters button
apply_filters = st.sidebar.button("Apply Filters")

# Main content area
try:
    # Initialize DataFrame
    df = pd.DataFrame()
    
    if apply_filters:
        # Construct filters dictionary
        filters = {}
        
        # Price filter
        if price_range[0] > 0 or price_range[1] < 5000000:
            min_price, max_price = price_range
            filters["price"] = (min_price, max_price)
        
        # Beds filter
        if beds_range[0] > 0 or beds_range[1] < 10:
            min_beds, max_beds = beds_range
            filters["beds"] = (min_beds, max_beds)
        
        # Baths filter
        if baths_range[0] > 0 or baths_range[1] < 10:
            min_baths, max_baths = baths_range
            filters["baths"] = (min_baths, max_baths)
        
        # Location filters
        if city:
            filters["city"] = city
        
        if state:
            filters["state"] = state
        
        if zip_code:
            filters["zip"] = zip_code
        
        # Advanced filters
        if min_sqft > 0:
            filters["sqft"] = (min_sqft, None)
        
        if min_rent_yield > 0:
            filters["rent_yield"] = (min_rent_yield / 100, None)
        
        if max_price_per_sqft > 0:
            filters["price_per_sqft"] = (0, max_price_per_sqft)
        
        if selected_mls_type != "Any":
            filters["mls_type"] = selected_mls_type
        
        if selected_status != "Any":
            filters["status"] = selected_status
        
        if min_walk_score > 0:
            filters["walk_score"] = (min_walk_score, None)
        
        # Get filtered data
        df = get_filtered_listings(db_path, filters)
        
        # Enrich dataframe with calculated fields
        df = enrich_dataframe(df)
        
        st.write(f"Found {len(df)} properties matching your criteria")
    else:
        # If no filters applied, show limited number of properties
        df = get_all_listings(db_path, limit=100)
        df = enrich_dataframe(df)
        st.write(f"Showing latest {len(df)} properties. Apply filters to refine results.")
    
    if df.empty:
        st.warning("No properties found matching your criteria.")
    else:
        # Add tabs for different views
        tab1, tab2, tab3 = st.tabs(["Table View", "Map View", "Visual Analysis"])
        
        with tab1:
            # Format columns for display
            display_df = df.copy()
            if 'price' in display_df.columns:
                display_df['price'] = display_df['price'].apply(format_currency)
            if 'price_per_sqft' in display_df.columns:
                display_df['price_per_sqft'] = display_df['price_per_sqft'].apply(format_currency)
            if 'estimated_rent' in display_df.columns:
                display_df['estimated_rent'] = display_df['estimated_rent'].apply(format_currency)
            if 'rent_yield' in display_df.columns:
                display_df['rent_yield'] = display_df['rent_yield'].apply(format_percentage)
            
            # Columns to display
            columns_to_show = [
                'address', 'city', 'state', 'zip', 'price', 'beds', 'baths', 
                'sqft', 'price_per_sqft', 'mls_type', 'walk_score', 
                'estimated_rent', 'rent_yield', 'status'
            ]
            columns_to_show = [col for col in columns_to_show if col in display_df.columns]
            
            # Display table with pagination
            st.dataframe(
                display_df[columns_to_show], 
                use_container_width=True,
                hide_index=True
            )
            
            # Export options
            st.write("### Export Data")
            col1, col2 = st.columns(2)
            
            with col1:
                # CSV download
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name="property_listings.csv",
                    mime="text/csv"
                )
            
            with col2:
                # Excel download
                excel_buffer = df.to_excel(index=False, engine='openpyxl')
                st.download_button(
                    label="Download as Excel",
                    data=excel_buffer,
                    file_name="property_listings.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with tab2:
            # Map view (if coordinates are available)
            st.write("### Property Map")
            
            # Check if we have coordinates (typically derived from ZIP code)
            has_location_data = False
            
            # For demonstration, we'll create random coordinates near Denver if not available
            if not has_location_data:
                st.info("Map view requires latitude and longitude data. Displaying a sample map with approximate locations.")
                
                # Create sample data for demonstration
                map_df = df.copy()
                # Center coordinates for Denver
                denver_lat, denver_lng = 39.7392, -104.9903
                
                # Generate random coordinates around Denver
                np.random.seed(42)  # For reproducibility
                num_records = len(map_df)
                map_df['lat'] = denver_lat + np.random.normal(0, 0.05, num_records)
                map_df['lon'] = denver_lng + np.random.normal(0, 0.05, num_records)
                
                # Format price for display
                map_df['price_str'] = map_df['price'].apply(lambda x: f"${x:,.0f}" if pd.notnull(x) else "N/A")
                
                # Display map
                st.map(
                    map_df[['lat', 'lon']], 
                    size=20,
                    zoom=10
                )
                
                st.write("""
                Note: This map shows approximate locations based on randomly generated coordinates around Denver.
                For accurate mapping, you would need to geocode your addresses to get exact latitude and longitude.
                """)
        
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

except Exception as e:
    st.error(f"Error: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
