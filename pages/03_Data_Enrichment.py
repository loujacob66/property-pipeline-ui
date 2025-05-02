import streamlit as st
import time
import os
import pandas as pd
from pathlib import Path
from utils.script_runner import (
    run_gmail_parser, 
    run_compass_enrichment, 
    run_walkscore_enrichment,
    get_script_progress
)
from utils.database import get_db_connection, get_all_listings
from utils.data_processing import get_properties_needing_enrichment

st.set_page_config(page_title="Data Enrichment", page_icon="🔄", layout="wide")
st.title("Data Enrichment")

# Get paths from session state
db_path = st.session_state.get('db_path', "")
scripts_path = st.session_state.get('scripts_path', "")

# Create tab layout
tab1, tab2, tab3, tab4 = st.tabs([
    "Enrichment Dashboard", 
    "Gmail Parser", 
    "Compass Enrichment", 
    "WalkScore Enrichment"
])

with tab1:
    st.header("Enrichment Dashboard")
    
    try:
        # Check database connection
        conn = get_db_connection(db_path)
        
        # Get data for dashboard
        df = get_all_listings(db_path)
        
        if df.empty:
            st.warning("No data found in the database.")
        else:
            # Display summary metrics
            st.subheader("Database Summary")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_properties = len(df)
                st.metric("Total Properties", f"{total_properties:,}")
            
            with col2:
                has_mls = df['mls_number'].notna().sum()
                st.metric("With MLS Data", f"{has_mls:,}", f"{has_mls/total_properties:.1%}")
            
            with col3:
                has_tax = df['tax_information'].notna().sum()
                st.metric("With Tax Info", f"{has_tax:,}", f"{has_tax/total_properties:.1%}")
            
            with col4:
                has_walkscore = df['walk_score'].notna().sum()
                st.metric("With WalkScore", f"{has_walkscore:,}", f"{has_walkscore/total_properties:.1%}")
            
            # Get properties needing enrichment
            enrichment_needed = get_properties_needing_enrichment(df)
            
            # Display enrichment needs
            st.subheader("Enrichment Needs")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                walkscore_missing = len(enrichment_needed['walkscore_missing'])
                st.metric("Need WalkScore Data", f"{walkscore_missing:,}")
                
                if walkscore_missing > 0:
                    st.button("Run WalkScore Enrichment", key="run_walkscore_dash", 
                              on_click=lambda: st.session_state.update({"active_tab": "WalkScore Enrichment"}))
            
            with col2:
                mls_missing = len(enrichment_needed['mls_missing'])
                st.metric("Need MLS Data", f"{mls_missing:,}")
                
                if mls_missing > 0:
                    st.button("Run Compass Enrichment", key="run_compass_dash", 
                              on_click=lambda: st.session_state.update({"active_tab": "Compass Enrichment"}))
            
            with col3:
                tax_missing = len(enrichment_needed['tax_missing'])
                st.metric("Need Tax Data", f"{tax_missing:,}")
                
                if tax_missing > 0:
                    st.button("Run Compass Enrichment", key="run_compass_tax_dash", 
                              on_click=lambda: st.session_state.update({"active_tab": "Compass Enrichment"}))
            
            # Data quality overview
            st.subheader("Data Quality Overview")
            
            # Calculate completeness percentages
            data_quality = {
                "Field": [],
                "Complete": [],
                "Missing": [],
                "Percentage": []
            }
            
            for column in [
                'address', 'price', 'beds', 'baths', 'sqft', 'city', 'state', 'zip',
                'mls_number', 'mls_type', 'tax_information', 'days_on_compass', 
                'walk_score', 'transit_score', 'bike_score', 'favorite', 'status'
            ]:
                if column in df.columns:
                    complete = df[column].notna().sum()
                    missing = total_properties - complete
                    percentage = complete / total_properties
                    
                    data_quality["Field"].append(column)
                    data_quality["Complete"].append(complete)
                    data_quality["Missing"].append(missing)
                    data_quality["Percentage"].append(f"{percentage:.1%}")
            
            # Create a DataFrame
            data_quality_df = pd.DataFrame(data_quality)
            
            # Display as a table
            st.dataframe(data_quality_df, use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.info(f"Make sure the database exists at: {db_path}")

with tab2:
    st.header("Gmail Parser")
    st.write("Parse property listings from Gmail emails. This will extract property data from emails and save it to your database.")
    
    # Path to Gmail parser script
    gmail_script_path = Path(scripts_path) / "multi_label_gmail_parser.py"
    
    if not gmail_script_path.exists():
        st.error(f"Gmail parser script not found at: {gmail_script_path}")
        st.info("Please check the Scripts Path in the sidebar configuration.")
    else:
        st.success(f"Gmail parser script found: {gmail_script_path}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            max_emails = st.number_input("Max Emails per Label", min_value=1, value=10)
            dry_run = st.checkbox("Dry Run (Preview without DB insertion)")
        
        with col2:
            config_dir = Path(scripts_path) / "config"
            config_file = None
            
            if config_dir.exists():
                config_options = list(config_dir.glob("*label_config*.json"))
                if config_options:
                    config_options = ["Select a config file..."] + [str(f) for f in config_options]
                    selected_config = st.selectbox("Config File", config_options)
                    
                    if selected_config != "Select a config file...":
                        config_file = selected_config
                else:
                    st.warning(f"No label config files found in: {config_dir}")
                    config_file = st.text_input("Config File Path", 
                                             value=str(config_dir / "label_config.json"))
            else:
                st.warning(f"Config directory not found: {config_dir}")
                config_file = st.text_input("Config File Path", 
                                         value=str(scripts_path / "config" / "label_config.json"))
        
        if st.button("Run Gmail Parser"):
            if not config_file or config_file == "Select a config file...":
                st.error("Please select or enter a config file path.")
            else:
                with st.spinner("Running Gmail Parser..."):
                    # Create a progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Run the script
                    result = run_gmail_parser(
                        script_path=gmail_script_path,
                        max_emails=max_emails, 
                        dry_run=dry_run, 
                        config=config_file
                    )
                    
                    # Update progress based on output
                    if result['stdout']:
                        progress_info = get_script_progress(result['stdout'])
                        if progress_info and progress_info['total'] > 0:
                            progress = min(progress_info['processed'] / progress_info['total'], 1.0)
                            progress_bar.progress(progress)
                            status_text.text(progress_info['last_message'])
                    
                    if result['returncode'] == 0:
                        st.success("Gmail Parser completed successfully")
                    else:
                        st.error("Gmail Parser failed")
                    
                    # Display output in expandable sections
                    with st.expander("Script Output", expanded=result['returncode'] != 0):
                        st.text_area("Output", result['stdout'], height=300)
                        
                        if result['stderr']:
                            st.error("Errors")
                            st.text_area("Error Output", result['stderr'], height=150)

with tab3:
    st.header("Compass Enrichment")
    st.write("Enrich property listings with data from Compass. This will add MLS numbers, tax information, and other details.")
    
    # Path to Compass enrichment script
    compass_script_path = Path(scripts_path) / "enrich_with_compass.py"
    
    if not compass_script_path.exists():
        st.error(f"Compass enrichment script not found at: {compass_script_path}")
        st.info("Please check the Scripts Path in the sidebar configuration.")
    else:
        st.success(f"Compass enrichment script found: {compass_script_path}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            limit = st.number_input("Limit (Max listings to process)", min_value=1, value=10)
            headless = st.checkbox("Headless Mode", 
                                 help="Run browser in headless mode (no visible window)")
            update_db = st.checkbox("Update Database", value=True,
                                  help="Update the database with enriched data")
        
        with col2:
            # Create output directory if it doesn't exist
            output_dir = Path(scripts_path) / "data" / "enriched"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Default output file with timestamp
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output_file = str(output_dir / f"enriched_listings_{timestamp}.json")
            
            output_file = st.text_input("Output File", value=default_output_file)
            address = st.text_input("Process Specific Address (optional)", value="")
        
        if st.button("Run Compass Enrichment"):
            with st.spinner("Running Compass Enrichment..."):
                # Create a progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Run the script
                result = run_compass_enrichment(
                    script_path=compass_script_path,
                    output=output_file,
                    limit=limit,
                    headless=headless,
                    update_db=update_db,
                    address=address if address else None
                )
                
                # Update progress based on output
                if result['stdout']:
                    progress_info = get_script_progress(result['stdout'])
                    if progress_info and progress_info['total'] > 0:
                        progress = min(progress_info['processed'] / progress_info['total'], 1.0)
                        progress_bar.progress(progress)
                        status_text.text(progress_info['last_message'])
                
                if result['returncode'] == 0:
                    st.success("Compass Enrichment completed successfully")
                else:
                    st.error("Compass Enrichment failed")
                
                # Display output in expandable sections
                with st.expander("Script Output", expanded=result['returncode'] != 0):
                    st.text_area("Output", result['stdout'], height=300)
                    
                    if result['stderr']:
                        st.error("Errors")
                        st.text_area("Error Output", result['stderr'], height=150)

with tab4:
    st.header("WalkScore Enrichment")
    st.write("Enrich property listings with WalkScore data. This will add Walk Score, Transit Score, and Bike Score information.")
    
    # Path to WalkScore enrichment script
    walkscore_script_path = Path(scripts_path) / "enrich_with_walkscore.py"
    
    if not walkscore_script_path.exists():
        st.error(f"WalkScore enrichment script not found at: {walkscore_script_path}")
        st.info("Please check the Scripts Path in the sidebar configuration.")
    else:
        st.success(f"WalkScore enrichment script found: {walkscore_script_path}")
        
        # Check for WalkScore API config
        walkscore_config_path = Path(scripts_path) / "config" / "walkscore_config.json"
        if not walkscore_config_path.exists():
            st.warning(f"WalkScore API configuration file not found: {walkscore_config_path}")
            st.info("Running the script will create a template config file that you'll need to fill in.")
        
        if st.button("Run WalkScore Enrichment"):
            with st.spinner("Running WalkScore Enrichment..."):
                # Create a progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Run the script
                result = run_walkscore_enrichment(walkscore_script_path)
                
                # Update progress based on output
                if result['stdout']:
                    progress_info = get_script_progress(result['stdout'])
                    if progress_info and progress_info['total'] > 0:
                        progress = min(progress_info['processed'] / progress_info['total'], 1.0)
                        progress_bar.progress(progress)
                        status_text.text(progress_info['last_message'])
                
                if result['returncode'] == 0:
                    st.success("WalkScore Enrichment completed successfully")
                else:
                    st.error("WalkScore Enrichment failed")
                
                # Display output in expandable sections
                with st.expander("Script Output", expanded=result['returncode'] != 0):
                    st.text_area("Output", result['stdout'], height=300)
                    
                    if result['stderr']:
                        st.error("Errors")
                        st.text_area("Error Output", result['stderr'], height=150)

# Handle tab switching from dashboard buttons
if 'active_tab' in st.session_state:
    active_tab = st.session_state['active_tab']
    if active_tab == "WalkScore Enrichment":
        # Switch to WalkScore tab
        st.experimental_set_query_params(tab="WalkScore Enrichment")
    elif active_tab == "Compass Enrichment":
        # Switch to Compass tab
        st.experimental_set_query_params(tab="Compass Enrichment")
    
    # Clear the active tab
    st.session_state.pop('active_tab', None)