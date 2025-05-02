import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.database import get_db_connection, get_all_listings, get_summary_stats
from utils.data_processing import enrich_dataframe, format_currency, format_percentage

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")
st.title("Property Pipeline Dashboard")

# Get paths from session state
db_path = st.session_state.get('db_path', "")

try:
    # Load summary stats
    stats = get_summary_stats(db_path)
    
    # Load all data (limited to latest 1000 for performance)
    df = get_all_listings(db_path, limit=1000)
    
    if df.empty:
        st.warning("No data found in the database.")
    else:
        # Enrich the dataframe with calculated fields
        df = enrich_dataframe(df)
        
        # Summary statistics
        st.header("Summary Statistics")
        
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
        
        # Visualizations
        st.header("Property Insights")
        
        tab1, tab2, tab3 = st.tabs(["Price Analysis", "Location Analysis", "Property Types"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Price distribution
                if 'price' in df.columns and df['price'].notna().any():
                    fig = px.histogram(
                        df, 
                        x="price", 
                        title="Price Distribution",
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(xaxis_title="Price ($)", yaxis_title="Number of Properties")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Price per sqft distribution
                if 'price_per_sqft' in df.columns and df['price_per_sqft'].notna().any():
                    fig = px.histogram(
                        df, 
                        x="price_per_sqft", 
                        title="Price per Sqft Distribution",
                        color_discrete_sequence=['#33CC99']
                    )
                    fig.update_layout(xaxis_title="Price per Sqft ($)", yaxis_title="Number of Properties")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Price category breakdown
            if 'price_category' in df.columns and df['price_category'].notna().any():
                price_counts = df['price_category'].value_counts().reset_index()
                price_counts.columns = ["price_category", "count"]
                
                fig = px.pie(
                    price_counts, 
                    values="count", 
                    names="price_category", 
                    title="Properties by Price Range",
                    color_discrete_sequence=px.colors.sequential.Blues
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # City distribution
                if 'city' in df.columns and df['city'].notna().any():
                    city_counts = stats['city_counts']
                    
                    fig = px.bar(
                        city_counts, 
                        x="city", 
                        y="count", 
                        title="Number of Properties by City (Top 10)",
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(xaxis_title="City", yaxis_title="Number of Properties")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Average price by city
                if 'city' in df.columns and df['price'].notna().any():
                    city_prices = stats['city_prices']
                    
                    fig = px.bar(
                        city_prices, 
                        x="city", 
                        y="avg_price", 
                        title="Average Price by City (Top 10)",
                        color_discrete_sequence=['#33CC99']
                    )
                    fig.update_layout(xaxis_title="City", yaxis_title="Average Price ($)")
                    st.plotly_chart(fig, use_container_width=True)
            
            # WalkScore analysis
            if 'walk_score' in df.columns and df['walk_score'].notna().any():
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.box(
                        df, 
                        x="walk_score", 
                        title="WalkScore Distribution",
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(xaxis_title="WalkScore", yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    if 'walk_score_category' in df.columns:
                        walk_counts = df['walk_score_category'].value_counts().reset_index()
                        walk_counts.columns = ["category", "count"]
                        
                        fig = px.pie(
                            walk_counts, 
                            values="count", 
                            names="category", 
                            title="Properties by Walkability",
                            color_discrete_sequence=px.colors.sequential.Greens
                        )
                        st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            col1, col2 = st.columns(2)
            
            with col1:
                # MLS Type breakdown
                if 'mls_type' in df.columns and df['mls_type'].notna().any():
                    mls_counts = df['mls_type'].value_counts().reset_index()
                    mls_counts.columns = ["mls_type", "count"]
                    
                    fig = px.pie(
                        mls_counts, 
                        values="count", 
                        names="mls_type", 
                        title="Property Types",
                        color_discrete_sequence=px.colors.sequential.Purples
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Beds distribution
                if 'beds' in df.columns and df['beds'].notna().any():
                    bed_counts = df['beds'].value_counts().sort_index().reset_index()
                    bed_counts.columns = ["beds", "count"]
                    
                    fig = px.bar(
                        bed_counts, 
                        x="beds", 
                        y="count", 
                        title="Number of Properties by Bedrooms",
                        color_discrete_sequence=['#9933CC']
                    )
                    fig.update_layout(xaxis_title="Bedrooms", yaxis_title="Number of Properties")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Beds vs Baths
            if all(x in df.columns for x in ['beds', 'baths']) and df['beds'].notna().any() and df['baths'].notna().any():
                beds_baths = df.groupby(['beds', 'baths']).size().reset_index(name='count')
                
                fig = px.scatter(
                    beds_baths, 
                    x="beds", 
                    y="baths", 
                    size="count", 
                    color="count",
                    title="Property Configurations: Beds vs. Baths",
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig.update_layout(xaxis_title="Bedrooms", yaxis_title="Bathrooms")
                st.plotly_chart(fig, use_container_width=True)
        
        # Recent listings
        st.header("Recent Listings")
        if "imported_at" in df.columns:
            recent_df = df.sort_values("imported_at", ascending=False).head(10)
            
            # Format columns for display
            display_df = recent_df.copy()
            if 'price' in display_df.columns:
                display_df['price'] = display_df['price'].apply(format_currency)
            if 'price_per_sqft' in display_df.columns:
                display_df['price_per_sqft'] = display_df['price_per_sqft'].apply(format_currency)
            if 'rent_yield' in display_df.columns:
                display_df['rent_yield'] = display_df['rent_yield'].apply(format_percentage)
            
            # Select columns to display
            columns_to_show = [
                'address', 'city', 'state', 'zip', 'price', 'beds', 'baths', 
                'sqft', 'price_per_sqft', 'mls_type', 'walk_score'
            ]
            columns_to_show = [col for col in columns_to_show if col in display_df.columns]
            
            st.dataframe(display_df[columns_to_show], use_container_width=True)
        else:
            st.dataframe(df.head(10), use_container_width=True)
        
        # Link to property explorer
        st.write("Use the Property Explorer page to see more listings and apply filters.")
        st.page_link("pages/02_Property_Explorer.py", label="Go to Property Explorer", icon="🔍")

except Exception as e:
    st.error(f"Error: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
