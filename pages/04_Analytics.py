import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from utils.database import get_db_connection, get_all_listings
from utils.data_processing import enrich_dataframe, format_currency, format_percentage
from utils.script_runner import run_cashflow_analyzer
from pathlib import Path

# Handle refresh after successful operations
if st.session_state.get('needs_refresh', False):
    st.session_state.pop('needs_refresh', None)
    st.rerun()

st.set_page_config(page_title="Analytics", page_icon="📈", layout="wide")
st.title("Analytics")

st.write("""
This page provides various tools for analyzing property data, including quick cashflow estimation, investment analysis, 
location analysis, and property comparisons. Use the tabs below to access different analysis tools.
""")

# Get paths from session state
db_path = st.session_state.get('db_path', "")
scripts_path = st.session_state.get('default_scripts_path', "../property-pipeline/scripts")

try:
    # Load all data
    df = get_all_listings(db_path)
    
    if df.empty:
        st.warning("No data found in the database.")
    else:
        # Enrich dataframe with calculated fields
        df = enrich_dataframe(df)
        
        # Filter options
        with st.sidebar:
            st.header("Filter Data")
            
            # Price filter
            price_range = st.slider(
                "Price Range ($)", 
                min_value=int(df['price'].min()) if 'price' in df.columns and not df['price'].isna().all() else 0,
                max_value=int(df['price'].max()) if 'price' in df.columns and not df['price'].isna().all() else 5000000,
                value=(0, int(df['price'].max()) if 'price' in df.columns and not df['price'].isna().all() else 5000000),
                step=50000,
                format="$%d"
            )
            
            # Location filter
            if 'city' in df.columns and not df['city'].isna().all():
                cities = df['city'].unique()
                cities = [city for city in cities if isinstance(city, str) and city.strip()]
                selected_cities = st.multiselect("Filter by City", ["All"] + sorted(cities), ["All"])
            
            # Apply filters
            apply_filters = st.button("Apply Filters")
            
            if apply_filters:
                # Price filter
                if 'price' in df.columns:
                    min_price, max_price = price_range
                    df = df[(df['price'] >= min_price) & (df['price'] <= max_price)]
                
                # City filter
                if 'city' in df.columns and selected_cities and "All" not in selected_cities:
                    df = df[df['city'].isin(selected_cities)]
                
                st.success(f"Filters applied. Showing {len(df)} properties.")
        
        # Main analysis tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Quick Cashflow Estimator",
            "Investment Analysis", 
            "Location Analysis", 
            "Property Comparison",
            "Data Export"
        ])
        
        with tab1:
            st.header("Quick Cashflow Estimator")
            st.write("""
            This tool provides quick cashflow estimates based on zipcode-level data from Zillow. 
            These estimates are useful for initial screening but may not reflect the exact cashflow potential of specific properties.
            """)
            if st.button("Go to Property Explorer"):
                st.switch_page("pages/02_Property_Explorer.py")

            cashflow_script_path = Path(scripts_path) / "cashflow_analyzer.py"

            if not cashflow_script_path.exists():
                st.error(f"Cashflow analyzer script not found at: {cashflow_script_path}")
                st.info("Please check the scripts path configuration.")
            else:
                cf_address = st.text_input("Property Address", key="cf_address_input")
                
                st.subheader("Financial Inputs")
                # Default values
                default_inputs = {
                    "down_payment": 325000.0,
                    "rate": 6.5,
                    "insurance": 4000.0,
                    "misc_monthly": 100.0,
                    "loan_term": 30
                }

                col_cf1, col_cf2 = st.columns(2)
                with col_cf1:
                    cf_down_payment = st.number_input("Down Payment ($)", value=default_inputs["down_payment"], format="%.2f", key="cf_down_payment")
                    cf_rate = st.number_input("Interest Rate (%)", value=default_inputs["rate"], format="%.2f", key="cf_rate")
                    cf_loan_term = st.number_input("Loan Term (Years)", value=default_inputs["loan_term"], step=1, key="cf_loan_term")
                with col_cf2:
                    cf_insurance = st.number_input("Annual Insurance ($)", value=default_inputs["insurance"], format="%.2f", key="cf_insurance")
                    cf_misc_monthly = st.number_input("Misc. Monthly Expenses ($)", value=default_inputs["misc_monthly"], format="%.2f", key="cf_misc_monthly")

                if st.button("Analyze Cashflow", key="analyze_cashflow_button"):
                    if not cf_address:
                        st.warning("Please enter a property address.")
                    else:
                        with st.spinner("Analyzing cashflow..."):
                            result = run_cashflow_analyzer(
                                script_path=str(cashflow_script_path),
                                address=cf_address,
                                down_payment=cf_down_payment,
                                rate=cf_rate,
                                insurance=cf_insurance,
                                misc_monthly=cf_misc_monthly,
                                loan_term=cf_loan_term,
                                db_path=db_path if db_path else None
                            )

                            if result['returncode'] == 0:
                                st.success("Cashflow analysis script run successfully.")
                                if result['stdout']:
                                    st.subheader("Analysis Result")
                                    st.text_area("Output", result['stdout'], height=200)
                            else:
                                st.error("Cashflow analysis script failed.")
                            
                            if result['stderr']:
                                st.error("Script Errors")
                                st.text_area("Error Output", result['stderr'], height=150)

        with tab2:
            st.header("Investment Analysis")
            
            # Rent Yield Analysis
            if 'rent_yield' in df.columns and df['rent_yield'].notna().any():
                col1, col2 = st.columns(2)
                
                with col1:
                    # Scatter plot of price vs. rent yield
                    fig = px.scatter(
                        df, 
                        x="price", 
                        y="rent_yield", 
                        title="Price vs. Rent Yield",
                        color="price_per_sqft" if 'price_per_sqft' in df.columns else None,
                        hover_data=['address', 'beds', 'baths', 'sqft', 'city'],
                        color_continuous_scale=px.colors.sequential.Viridis
                    )
                    fig.update_layout(
                        xaxis_title="Price ($)",
                        yaxis_title="Annual Rent Yield",
                        yaxis=dict(tickformat=".1%")
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Histogram of rent yield
                    fig = px.histogram(
                        df, 
                        x="rent_yield",
                        title="Rent Yield Distribution",
                        nbins=20,
                        color_discrete_sequence=['#3366CC']
                    )
                    fig.update_layout(
                        xaxis_title="Annual Rent Yield",
                        yaxis_title="Number of Properties",
                        xaxis=dict(tickformat=".1%")
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Top rent yield properties
                st.subheader("Top 10 Properties by Rent Yield")
                top_yield_df = df.sort_values("rent_yield", ascending=False).head(10)
                
                # Format for display
                display_df = top_yield_df.copy()
                if 'price' in display_df.columns:
                    display_df['price'] = display_df['price'].apply(format_currency)
                if 'estimated_rent' in display_df.columns:
                    display_df['estimated_rent'] = display_df['estimated_rent'].apply(format_currency)
                if 'rent_yield' in display_df.columns:
                    display_df['rent_yield'] = display_df['rent_yield'].apply(format_percentage)
                
                # Select columns to show
                columns_to_show = [
                    'address', 'city', 'price', 'estimated_rent', 'rent_yield', 
                    'beds', 'baths', 'sqft', 'price_per_sqft'
                ]
                columns_to_show = [col for col in columns_to_show if col in display_df.columns]
                
                st.dataframe(display_df[columns_to_show], use_container_width=True, hide_index=True)
                
                # Yield by property type
                if 'mls_type' in df.columns and df['mls_type'].notna().any():
                    st.subheader("Average Rent Yield by Property Type")
                    
                    type_yield = df.groupby('mls_type')['rent_yield'].agg(['mean', 'count']).reset_index()
                    type_yield = type_yield[type_yield['count'] >= 3]  # Only show types with at least 3 properties
                    
                    if not type_yield.empty:
                        type_yield = type_yield.sort_values('mean', ascending=False)
                        
                        fig = px.bar(
                            type_yield,
                            x='mls_type',
                            y='mean',
                            title="Average Rent Yield by Property Type",
                            text='count',  # Show count on bars
                            color='mean',
                            color_continuous_scale=px.colors.sequential.Viridis
                        )
                        fig.update_layout(
                            xaxis_title="Property Type",
                            yaxis_title="Average Annual Rent Yield",
                            yaxis=dict(tickformat=".1%")
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Rent yield data not available. Please run the Gmail parser to import property listings with rent estimates.")
        
        with tab3:
            st.header("Location Analysis")
            
            # WalkScore Analysis
            if 'walk_score' in df.columns and df['walk_score'].notna().any():
                col1, col2 = st.columns(2)
                
                with col1:
                    # WalkScore distribution
                    fig = px.histogram(
                        df,
                        x="walk_score",
                        title="WalkScore Distribution",
                        nbins=20,
                        color_discrete_sequence=['#33CC99']
                    )
                    fig.update_layout(
                        xaxis_title="WalkScore (0-100)",
                        yaxis_title="Number of Properties"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # WalkScore impact on price
                    fig = px.scatter(
                        df,
                        x="walk_score",
                        y="price",
                        title="WalkScore vs. Price",
                        hover_data=['address', 'city', 'zip'],
                        color="transit_score" if 'transit_score' in df.columns else None,
                        color_continuous_scale=px.colors.sequential.Viridis
                    )
                    fig.update_layout(
                        xaxis_title="WalkScore (0-100)",
                        yaxis_title="Price ($)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # All transportation scores
                if all(x in df.columns for x in ['walk_score', 'transit_score', 'bike_score']):
                    st.subheader("Transportation Scores Comparison")
                    
                    # Reshape data for grouped bar chart
                    score_data = []
                    for _, row in df.iterrows():
                        if pd.notna(row['walk_score']):
                            score_data.append({
                                'address': row['address'],
                                'score_type': 'Walk Score',
                                'score': row['walk_score']
                            })
                        
                        if pd.notna(row['transit_score']):
                            score_data.append({
                                'address': row['address'],
                                'score_type': 'Transit Score',
                                'score': row['transit_score']
                            })
                        
                        if pd.notna(row['bike_score']):
                            score_data.append({
                                'address': row['address'],
                                'score_type': 'Bike Score',
                                'score': row['bike_score']
                            })
                    
                    score_df = pd.DataFrame(score_data)
                    
                    # Calculate averages
                    avg_scores = score_df.groupby('score_type')['score'].mean().reset_index()
                    
                    # Create bar chart
                    fig = px.bar(
                        avg_scores,
                        x='score_type',
                        y='score',
                        title="Average Transportation Scores",
                        color='score_type',
                        text='score'
                    )
                    fig.update_layout(
                        xaxis_title="",
                        yaxis_title="Average Score (0-100)"
                    )
                    # Round the text values
                    fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # City Analysis
            if 'city' in df.columns and df['city'].notna().any():
                st.subheader("City Analysis")
                
                # City distribution
                city_counts = df['city'].value_counts().reset_index()
                city_counts.columns = ['city', 'count']
                top_cities = city_counts.head(10)
                
                fig = px.bar(
                    top_cities,
                    x='city',
                    y='count',
                    title="Number of Properties by City (Top 10)",
                    color='count',
                    color_continuous_scale=px.colors.sequential.Blues
                )
                fig.update_layout(
                    xaxis_title="City",
                    yaxis_title="Number of Properties"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Price analysis by city
                if 'price' in df.columns and df['price'].notna().any():
                    city_price = df.groupby('city')['price'].agg(['mean', 'median', 'count']).reset_index()
                    city_price = city_price[city_price['count'] >= 3]  # Only cities with at least 3 properties
                    city_price = city_price.sort_values('mean', ascending=False).head(10)
                    
                    fig = px.bar(
                        city_price,
                        x='city',
                        y='mean',
                        title="Average Property Price by City (Top 10)",
                        text='count',  # Show count on bars
                        color='mean',
                        color_continuous_scale=px.colors.sequential.Greens
                    )
                    fig.update_layout(
                        xaxis_title="City",
                        yaxis_title="Average Price ($)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            st.header("Property Comparison")
            
            # Properties by price range
            if 'price_category' in df.columns and df['price_category'].notna().any():
                st.subheader("Properties by Price Range")
                
                price_counts = df['price_category'].value_counts().reset_index()
                price_counts.columns = ['price_category', 'count']
                
                fig = px.pie(
                    price_counts,
                    values='count',
                    names='price_category',
                    title="Properties by Price Range",
                    color_discrete_sequence=px.colors.sequential.Blues
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Price per Sqft Analysis
            if 'price_per_sqft' in df.columns and df['price_per_sqft'].notna().any():
                st.subheader("Price per Square Foot Analysis")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Price per sqft distribution
                    fig = px.histogram(
                        df,
                        x='price_per_sqft',
                        title="Price per Sqft Distribution",
                        nbins=20,
                        color_discrete_sequence=['#CC6633']
                    )
                    fig.update_layout(
                        xaxis_title="Price per Sqft ($)",
                        yaxis_title="Number of Properties"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Price per sqft vs. total sqft
                    fig = px.scatter(
                        df,
                        x='sqft',
                        y='price_per_sqft',
                        title="Price per Sqft vs. Total Sqft",
                        hover_data=['address', 'beds', 'baths', 'price'],
                        color='price',
                        color_continuous_scale=px.colors.sequential.Reds
                    )
                    fig.update_layout(
                        xaxis_title="Square Footage",
                        yaxis_title="Price per Sqft ($)"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Price per sqft by city
                if 'city' in df.columns and df['city'].notna().any():
                    city_ppsf = df.groupby('city')['price_per_sqft'].agg(['mean', 'count']).reset_index()
                    city_ppsf = city_ppsf[city_ppsf['count'] >= 3]  # Only cities with at least 3 properties
                    city_ppsf = city_ppsf.sort_values('mean', ascending=False).head(10)
                    
                    fig = px.bar(
                        city_ppsf,
                        x='city',
                        y='mean',
                        title="Average Price per Sqft by City (Top 10)",
                        text='count',  # Show count on bars
                        color='mean',
                        color_continuous_scale=px.colors.sequential.Oranges
                    )
                    fig.update_layout(
                        xaxis_title="City",
                        yaxis_title="Average Price per Sqft ($)"
                    )
                    fig.update_traces(textposition='outside')
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            # Bed/Bath Analysis
            if all(x in df.columns for x in ['beds', 'baths']) and df['beds'].notna().any() and df['baths'].notna().any():
                st.subheader("Bedroom & Bathroom Analysis")
                
                # Create a configuration matrix of beds vs baths
                bed_bath_matrix = pd.crosstab(df['beds'], df['baths'])
                
                # Convert to heatmap format
                bed_bath_data = []
                for bed, row in bed_bath_matrix.iterrows():
                    for bath, count in row.items():
                        if count > 0:
                            bed_bath_data.append({
                                'beds': bed,
                                'baths': bath,
                                'count': count
                            })
                
                bed_bath_df = pd.DataFrame(bed_bath_data)
                
                # Create heatmap
                fig = px.density_heatmap(
                    bed_bath_df,
                    x='beds',
                    y='baths',
                    z='count',
                    title="Property Configuration: Beds vs. Baths",
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig.update_layout(
                    xaxis_title="Bedrooms",
                    yaxis_title="Bathrooms"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Average price by beds
                if 'price' in df.columns and df['price'].notna().any():
                    bed_price = df.groupby('beds')['price'].agg(['mean', 'count']).reset_index()
                    bed_price = bed_price[bed_price['count'] >= 3]  # Only bed counts with at least 3 properties
                    
                    fig = px.bar(
                        bed_price,
                        x='beds',
                        y='mean',
                        title="Average Price by Number of Bedrooms",
                        text='count',  # Show count on bars
                        color='mean',
                        color_continuous_scale=px.colors.sequential.Blues
                    )
                    fig.update_layout(
                        xaxis_title="Bedrooms",
                        yaxis_title="Average Price ($)"
                    )
                    fig.update_traces(textposition='outside')
                    
                    st.plotly_chart(fig, use_container_width=True)
        
        with tab5:
            st.header("Data Export")
            
            st.write("""
            Export filtered property data for further analysis. The data will be exported in the selected format 
            with all calculated fields and enrichments.
            """)
            
            # Select fields to export
            st.subheader("Select Fields to Export")
            
            # Group fields by category
            field_categories = {
                "Property Info": [
                    'address', 'city', 'state', 'zip', 'price', 'beds', 'baths', 'sqft', 
                    'price_per_sqft', 'mls_type', 'mls_number'
                ],
                "Financial Info": [
                    'estimated_rent', 'rent_yield', 'tax_information', 'yield_category'
                ],
                "Location Info": [
                    'walk_score', 'transit_score', 'bike_score', 'walk_score_category'
                ],
                "Status Info": [
                    'status', 'days_on_compass', 'last_updated', 'imported_at'
                ]
            }
            
            # Create checkboxes for each field category
            selected_fields = []
            for category, fields in field_categories.items():
                with st.expander(category, expanded=True):
                    for field in fields:
                        if field in df.columns:
                            if st.checkbox(field, value=True, key=f"export_{field}"):
                                selected_fields.append(field)
            
            # Export format
            export_format = st.radio("Export Format", ["CSV", "Excel", "JSON"])
            
            if st.button("Export Data"):
                if not selected_fields:
                    st.error("Please select at least one field to export.")
                else:
                    # Create export dataframe with selected fields
                    export_df = df[selected_fields]
                    
                    if export_format == "CSV":
                        csv = export_df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name="property_listings_export.csv",
                            mime="text/csv"
                        )
                    elif export_format == "Excel":
                        excel_buffer = export_df.to_excel(index=False, engine='openpyxl')
                        st.download_button(
                            label="Download Excel",
                            data=excel_buffer,
                            file_name="property_listings_export.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    elif export_format == "JSON":
                        json_str = export_df.to_json(orient="records")
                        st.download_button(
                            label="Download JSON",
                            data=json_str,
                            file_name="property_listings_export.json",
                            mime="application/json"
                        )
                    
                    st.success(f"Data exported successfully with {len(export_df)} properties and {len(selected_fields)} fields.")

except Exception as e:
    st.error(f"Error: {e}")
    st.info(f"Make sure the database exists at: {db_path}")
