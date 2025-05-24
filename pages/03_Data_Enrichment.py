import streamlit as st
import time
import os
import pandas as pd
from pathlib import Path
from utils.script_runner import (
    run_gmail_parser, 
    run_compass_enrichment, 
    run_walkscore_enrichment,
    run_cashflow_enrichment,
    get_script_progress
)
from utils.database import get_db_connection, get_all_listings, get_blacklisted_addresses
from utils.data_processing import get_properties_needing_enrichment
from utils.table_config import get_column_config, get_compass_enrichment_columns, get_walkscore_enrichment_columns
from utils.table_styles import get_table_styles

st.set_page_config(page_title="Data Enrichment", page_icon="🔄", layout="wide")
st.title("Data Enrichment")

# Get paths from session state
db_path = st.session_state.get('db_path', "")
scripts_path = st.session_state.get('default_scripts_path', "../property-pipeline/scripts")
config_path = st.session_state.get('default_config_path', "../property-pipeline/config")

# Define tab names and determine the active one from query params
tab_names = [
    "Enrichment Dashboard", 
    "Gmail Parser", 
    "Compass Enrichment", 
    "WalkScore Enrichment",
    "Cashflow Enrichment",
    "Blacklist Management"
]
current_tab_name = st.query_params.get("tab", tab_names[0])
if current_tab_name not in tab_names:
    current_tab_name = tab_names[0]

# Create tabs instead of radio buttons
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_names)

with tab1:
    st.header("Enrichment Dashboard")
    
    try:
        conn = get_db_connection(db_path)
        df = get_all_listings(db_path)
        
        if df.empty:
            st.warning("No data found in the database.")
        else:
            st.subheader("Database Summary")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                total_properties = len(df)
                st.metric("Total Properties", f"{total_properties:,}")
            
            with col2:
                has_mls = df['mls_number'].notna().sum()
                st.metric("With MLS Data", f"{has_mls:,}", f"{has_mls/total_properties:.1%}" if total_properties > 0 else "0.0%")
            
            with col3:
                has_tax = df['tax_information'].notna().sum()
                st.metric("With Tax Info", f"{has_tax:,}", f"{has_tax/total_properties:.1%}" if total_properties > 0 else "0.0%")
            
            with col4:
                has_walkscore = df['walk_score'].notna().sum()
                st.metric("With WalkScore", f"{has_walkscore:,}", f"{has_walkscore/total_properties:.1%}" if total_properties > 0 else "0.0%")

            with col5:
                if 'estimated_monthly_cashflow' in df.columns:
                    has_cashflow = df['estimated_monthly_cashflow'].notna().sum()
                    st.metric("With Cashflow Data", f"{has_cashflow:,}", f"{has_cashflow/total_properties:.1%}" if total_properties > 0 else "0.0%")
                else:
                    st.metric("With Cashflow Data", "0", "0.0%")

            enrichment_needed = get_properties_needing_enrichment(df)
            
            st.subheader("Enrichment Needs")
            
            col1, col2, col3, col4 = st.columns(4)
            
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

            with col4:
                cashflow_missing_count = len(enrichment_needed.get('cashflow_missing', []))
                st.metric("Need Cashflow Data", f"{cashflow_missing_count:,}")

                if cashflow_missing_count > 0:
                    st.button("Run Cashflow Enrichment", key="run_cashflow_dash",
                              on_click=lambda: st.session_state.update({"active_tab": "Cashflow Enrichment"}))

            if 'active_tab' in st.session_state:
                if st.session_state['active_tab'] == 'Compass Enrichment':
                    st.info('Switching to Compass Enrichment...')
                elif st.session_state['active_tab'] == 'WalkScore Enrichment':
                    st.info('Switching to WalkScore Enrichment...')
                elif st.session_state['active_tab'] == 'Cashflow Enrichment':
                    st.info('Switching to Cashflow Enrichment...')
            
    except Exception as e:
        st.error(f"Error connecting to database or processing data: {e}")
        st.info(f"Make sure the database exists at: {db_path} and the schema is as expected.")

with tab2:
    st.header("Gmail Parser")
    st.write("Parse property listings from Gmail emails. This will extract property data from emails and save it to your database.")
    
    gmail_script_path = Path(scripts_path) / "multi_label_gmail_parser.py"
    config_dir = Path(config_path)
    config_file = st.text_input("Config File Path", value=str(config_dir / "label_config.json"))
    
    if not gmail_script_path.exists():
        st.error(f"Gmail parser script not found at: {gmail_script_path}")
        st.info("Please check the scripts path configuration.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            max_emails = st.number_input("Max Emails per Label", min_value=1, value=10)
            dry_run = st.checkbox("Dry Run (Preview without DB insertion)")
        
        with col2:
            pass
        
        if st.button("Run Gmail Parser"):
            if not config_file:
                st.error("Please enter a config file path.")
            else:
                with st.spinner("Running Gmail Parser..."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    result = run_gmail_parser(
                        script_path=gmail_script_path,
                        max_emails=max_emails, 
                        dry_run=dry_run, 
                        config=config_file
                    )
                    
                    if result['stdout']:
                        progress_info = get_script_progress(result['stdout'])
                        if progress_info and progress_info['total'] > 0:
                            progress = min(progress_info['processed'] / progress_info['total'], 1.0)
                            progress_bar.progress(progress)
                            status_text.text(progress_info['last_message'])
                    
                    if result['returncode'] == 0:
                        st.success("Gmail Parser completed successfully")
                        # Add refresh after successful operation
                        st.session_state['needs_refresh'] = True
                        st.rerun()
                    else:
                        st.error("Gmail Parser failed")
                    
                    with st.expander("Script Output", expanded=result['returncode'] != 0):
                        st.text_area("Output", result['stdout'], height=300)
                        
                        if result['stderr']:
                            st.error("Errors")
                            st.text_area("Error Output", result['stderr'], height=150)

with tab3:
    st.header("Compass Enrichment")
    st.write("Enrich property listings with data from Compass. This will add MLS numbers, tax information, and other details.")
    
    # Add instructions
    st.info("""
    **How to use:**
    1. **Select specific properties** using the checkboxes in the table below, OR
    2. **Use the limit parameter** to process the first N properties (if no checkboxes are selected)
    
    Then click "Run Compass Enrichment" to start the process.
    """)
    
    compass_script_path = Path(scripts_path) / "enrich_with_compass.py"
    
    if not compass_script_path.exists():
        st.error(f"Compass enrichment script not found at: {compass_script_path}")
        st.info("Please check the scripts path configuration.")
    else:
        # Get all properties
        try:
            conn = get_db_connection(db_path)
            df = get_all_listings(db_path)
            
            if df.empty:
                st.warning("No properties found in the database.")
            else:
                # Sort by db_updated_at in descending order (most recent first)
                if 'db_updated_at' in df.columns:
                    df['db_updated_at'] = pd.to_datetime(df['db_updated_at'], errors='coerce')
                    df = df.sort_values('db_updated_at', ascending=False)
                
                df['selected'] = False  # Add checkbox column
                
                # Get column configuration and selected columns
                column_config = get_column_config(interactive=True)
                # Override the db_updated_at width specifically for this table
                column_config['db_updated_at'] = st.column_config.DatetimeColumn(
                    'DB Update',
                    format="MM/DD/YY",
                    width=110  # Increased from 90 to 110
                )
                selected_columns = get_compass_enrichment_columns()
                
                # Apply table styles
                st.markdown(get_table_styles(), unsafe_allow_html=True)
                
                st.subheader("Properties")
                edited_df = st.data_editor(
                    df[selected_columns],
                    column_config=column_config,
                    use_container_width=True,
                    hide_index=True,
                    key="compass_enrichment_table"
                )
                
                # Get selected addresses
                selected_addresses = edited_df[edited_df['selected']]['address'].tolist()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    limit = st.number_input("Limit (Used only when no addresses are selected)", min_value=1, value=10)
                    update_db = st.checkbox("Update Database", value=True,
                                          help="Update the database with enriched data")
                
                with col2:
                    output_dir = Path(config_path) / "enriched"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    import datetime
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    default_output_file = str(output_dir / f"enriched_listings_{timestamp}.json")
                    
                    output_file = st.text_input("Output File", value=default_output_file)
                
                if st.button("Run Compass Enrichment"):
                    with st.spinner("Running Compass Enrichment..."):
                        # Create a progress bar
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Determine what to process
                        if selected_addresses:
                            # Process selected addresses
                            addresses_to_process = selected_addresses
                            status_text.text(f"Processing {len(addresses_to_process)} selected addresses...")
                        else:
                            # Get properties that need enrichment
                            enrichment_needed = get_properties_needing_enrichment(df)
                            properties_needing_enrichment = pd.concat([
                                enrichment_needed['mls_missing'],
                                enrichment_needed['tax_missing']
                            ]).drop_duplicates(subset=['address'])
                            
                            # Take the first N properties that need enrichment
                            addresses_to_process = properties_needing_enrichment['address'].head(limit).tolist()
                            status_text.text(f"Processing {len(addresses_to_process)} properties that need enrichment...")
                        
                        # Run the script for each address
                        for i, address in enumerate(addresses_to_process):
                            try:
                                # Update progress
                                progress = (i + 1) / len(addresses_to_process)
                                progress_bar.progress(progress)
                                status_text.text(f"Processing {i + 1} of {len(addresses_to_process)}: {address}")
                                
                                # Run the script with the address parameter
                                result = run_compass_enrichment(
                                    script_path=compass_script_path,
                                    output=output_file,
                                    limit=None if selected_addresses else limit,  # Only use limit if no addresses selected
                                    update_db=update_db,
                                    address=address
                                )
                                
                                if result['returncode'] == 0:
                                    st.success(f"Compass Enrichment completed successfully for {address}")
                                    # Add refresh after successful operation
                                    st.session_state['needs_refresh'] = True
                                else:
                                    st.error(f"Compass Enrichment failed for {address}")
                                
                                # Display output in expandable sections
                                with st.expander(f"Script Output for {address}", expanded=result['returncode'] != 0):
                                    # Separate stdout into info/warning and error messages
                                    stdout_lines = result['stdout'].splitlines()
                                    info_warning_output = "\n".join([line for line in stdout_lines if " - INFO -" in line or " - WARNING -" in line])
                                    error_output = "\n".join([line for line in stdout_lines if " - ERROR -" in line])

                                    if info_warning_output:
                                        st.text_area("Output", info_warning_output, height=300, key=f"output_{address}")

                                    if result['stderr'] or error_output:
                                        st.error("Errors")
                                        # Combine stderr and parsed error messages from stdout
                                        combined_error_output = (result['stderr'] + "\n" + error_output).strip()
                                        st.text_area("Error Output", combined_error_output, height=150, key=f"error_{address}")
                            except Exception as e:
                                st.error(f"Error processing {address}: {str(e)}")
                        
                        # Add a refresh button instead of automatic refresh
                        if st.button("Refresh Page to Show Updated Data"):
                            st.rerun()
                
        except Exception as e:
            st.error(f"Error loading properties: {e}")
            st.info("Please check your database connection and try again.")

with tab4:
    st.header("WalkScore Enrichment")
    st.write("Enrich property listings with WalkScore data. This will add Walk Score, Transit Score, and Bike Score information.")
    
    walkscore_script_path = Path(scripts_path) / "enrich_with_walkscore.py"
    walkscore_config_path = Path(config_path) / "walkscore_config.json"
    
    if not walkscore_script_path.exists():
        st.error(f"WalkScore enrichment script not found at: {walkscore_script_path}")
        st.info("Please check the scripts path configuration.")
    else:
        # Check for WalkScore API config
        if not walkscore_config_path.exists():
            st.warning(f"WalkScore API configuration file not found: {walkscore_config_path}")
            st.info("Running the script will create a template config file that you'll need to fill in.")
        
        # Get all properties
        try:
            conn = get_db_connection(db_path)
            df = get_all_listings(db_path)
            
            if df.empty:
                st.warning("No properties found in the database.")
            else:
                # Sort by db_updated_at in descending order (most recent first)
                if 'db_updated_at' in df.columns:
                    df['db_updated_at'] = pd.to_datetime(df['db_updated_at'], errors='coerce')
                    df = df.sort_values('db_updated_at', ascending=False)
                
                df['selected'] = False  # Add checkbox column
                
                # Get column configuration and selected columns
                column_config = get_column_config(interactive=True)
                selected_columns = get_walkscore_enrichment_columns()
                
                # Apply table styles
                st.markdown(get_table_styles(), unsafe_allow_html=True)
                
                st.subheader("Properties")
                edited_df = st.data_editor(
                    df[selected_columns],
                    column_config=column_config,
                    use_container_width=True,
                    hide_index=True,
                    key="walkscore_enrichment_table"
                )
                
                # Get selected addresses
                selected_addresses = edited_df[edited_df['selected']]['address'].tolist()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    limit = st.number_input("Limit (Used only when no addresses are selected)", min_value=1, value=10, key="walkscore_limit")
                    dry_run = st.checkbox("Dry Run (Preview without DB insertion)", key="walkscore_dry_run")
                
                with col2:
                    pass
                
                if st.button("Run WalkScore Enrichment"):
                    with st.spinner("Running WalkScore Enrichment..."):
                        # Create a progress bar
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Determine what to process
                        if selected_addresses:
                            # Process selected addresses
                            addresses_to_process = selected_addresses
                            status_text.text(f"Processing {len(addresses_to_process)} selected addresses...")
                        else:
                            # Get properties that need enrichment
                            enrichment_needed = get_properties_needing_enrichment(df)
                            properties_needing_enrichment = pd.concat([
                                enrichment_needed['walkscore_missing'],
                                enrichment_needed['transit_missing'],
                                enrichment_needed['bike_missing']
                            ]).drop_duplicates(subset=['address'])
                            
                            # Take the first N properties that need enrichment
                            addresses_to_process = properties_needing_enrichment['address'].head(limit).tolist()
                            status_text.text(f"Processing {len(addresses_to_process)} properties that need enrichment...")
                        
                        # Run the script for each address
                        for i, address in enumerate(addresses_to_process):
                            try:
                                # Update progress
                                progress = (i + 1) / len(addresses_to_process)
                                progress_bar.progress(progress)
                                status_text.text(f"Processing {i + 1} of {len(addresses_to_process)}: {address}")
                                
                                # Run the script with the address parameter
                                result = run_walkscore_enrichment(
                                    script_path=walkscore_script_path,
                                    address=address,
                                    limit=limit if not selected_addresses else None,
                                    dry_run=dry_run
                                )
                                
                                # Display output in expandable sections
                                with st.expander(f"Results for {address}", expanded=result['returncode'] != 0):
                                    # Separate stdout into info/warning and error messages
                                    stdout_lines = result['stdout'].splitlines()
                                    info_warning_output = "\n".join([line for line in stdout_lines if " - INFO -" in line or " - WARNING -" in line])
                                    error_output = "\n".join([line for line in stdout_lines if " - ERROR -" in line])

                                    if info_warning_output:
                                        st.text_area("Script Output", info_warning_output, height=300, key=f"output_{address}")

                                    if result['stderr'] or error_output:
                                        st.error("Script encountered issues")
                                        # Combine stderr and parsed error messages from stdout
                                        combined_error_output = (result['stderr'] + "\n" + error_output).strip()
                                        st.text_area("Details", combined_error_output, height=150, key=f"error_{address}")
                                
                                if result['returncode'] == 0:
                                    st.success(f"WalkScore Enrichment completed successfully for {address}")
                                else:
                                    st.error(f"WalkScore Enrichment failed for {address}")
                                
                            except Exception as e:
                                st.error(f"Error processing {address}: {str(e)}")
                        
                        # Add a refresh button
                        if st.button("Refresh Page to Show Updated Data"):
                            st.session_state['needs_refresh'] = True
                            st.rerun()
        except Exception as e:
            st.error(f"Error loading properties: {e}")
            st.info("Please check your database connection and try again.")

# Display any stored results at the top of the page
if 'walkscore_results' in st.session_state:
    st.subheader("Recent WalkScore Enrichment Results")
    for result_data in st.session_state['walkscore_results']:
        # Only show results from the last 5 minutes
        if time.time() - result_data['timestamp'] < 300:  # 5 minutes
            with st.expander(f"Results for {result_data['address']}", expanded=True):
                result = result_data['result']
                stdout_lines = result['stdout'].splitlines()
                info_warning_output = "\n".join([line for line in stdout_lines if " - INFO -" in line or " - WARNING -" in line])
                error_output = "\n".join([line for line in stdout_lines if " - ERROR -" in line])

                if info_warning_output:
                    st.text_area("Output", info_warning_output, height=300, key=f"stored_output_{result_data['address']}")

                if result['stderr'] or error_output:
                    st.error("Errors")
                    combined_error_output = (result['stderr'] + "\n" + error_output).strip()
                    st.text_area("Error Output", combined_error_output, height=150, key=f"stored_error_{result_data['address']}")

with tab5:
    st.header("Cashflow Enrichment")
    st.write("Enrich property listings with estimated monthly cashflow based on financial parameters.")

    cashflow_script_path = Path(scripts_path) / "enrich_with_cashflow.py"
    default_cashflow_config_path = Path(config_path) / "cashflow_config.json"

    if not cashflow_script_path.exists():
        st.error(f"Cashflow enrichment script not found at: {cashflow_script_path}")
        st.info("Please check the scripts path configuration.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            cashflow_config_file = st.text_input("Config File Path", value=str(default_cashflow_config_path))
            limit = st.number_input("Max listings to process (Limit)", min_value=1, value=10, key="cashflow_limit")
            dry_run = st.checkbox("Dry Run (Preview without DB insertion)", key="cashflow_dry_run")
        
        with col2:
            force_update = st.checkbox("Force Update (Recalculate existing values)", key="cashflow_force_update")
            address = st.text_input("Process Specific Address (optional)", value="", key="cashflow_address")

        if st.button("Run Cashflow Enrichment"):
            if not cashflow_config_file:
                st.error("Please enter a config file path for cashflow enrichment.")
            else:
                with st.spinner("Running Cashflow Enrichment..."):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    result = run_cashflow_enrichment(
                        script_path=str(cashflow_script_path),
                        config_path=cashflow_config_file,
                        db_path=db_path if db_path else None,
                        limit=limit, 
                        dry_run=dry_run,
                        force_update=force_update,
                        address=address if address else None
                    )
                    
                    if result['stdout']:
                        progress_info = get_script_progress(result['stdout'])
                        if progress_info and progress_info['total'] > 0:
                            progress = min(progress_info['processed'] / progress_info['total'], 1.0)
                            progress_bar.progress(progress)
                            status_text.text(progress_info['last_message'])
                    
                    if result['returncode'] == 0:
                        st.success("Cashflow Enrichment completed successfully")
                        # Add refresh after successful operation
                        st.session_state['needs_refresh'] = True
                        st.rerun()
                    else:
                        st.error("Cashflow Enrichment failed")
                    
                    with st.expander("Script Output", expanded=result['returncode'] != 0):
                        stdout_lines = result['stdout'].splitlines()
                        info_warning_output = "\n".join([line for line in stdout_lines if " - INFO -" in line or " - WARNING -" in line])
                        error_output_stdout = "\n".join([line for line in stdout_lines if " - ERROR -" in line])

                        if info_warning_output:
                            st.text_area("Output", info_warning_output, height=300)

                        if result['stderr'] or error_output_stdout:
                            st.error("Errors")
                            combined_error_output = (result['stderr'] + "\n" + error_output_stdout).strip()
                            st.text_area("Error Output", combined_error_output, height=150)

with tab6:
    st.header("Blacklist Management")
    
    # Show current blacklist
    st.subheader("Current Blacklist")
    blacklist_df = get_blacklisted_addresses(db_path)
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
            st.session_state['needs_refresh'] = True
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
            st.session_state['needs_refresh'] = True
            st.rerun()
    else:
        st.info("No addresses available to remove from blacklist.")

    # --- Run Blacklist Expired Script ---
    st.subheader("Clean Up Expired Blacklist Entries")
    st.write("Run the script to automatically remove expired addresses from the blacklist based on criteria defined in the script.")

    # Initialize session state for script output
    if 'blacklist_expired_output' not in st.session_state:
        st.session_state['blacklist_expired_output'] = ""

    # Add Dry Run checkbox
    run_expired_dry_run = st.checkbox("Dry Run (do not actually modify the database)", key="run_expired_dry_run_checkbox")

    if st.button("Run Blacklist Expired Script", key="run_blacklist_expired_script_button"):
         # Define the path to the blacklist_expired_address.py script
        blacklist_expired_script_path = Path(scripts_path) / "blacklist_address_expired.py"

        if not blacklist_expired_script_path.exists():
            st.session_state['blacklist_expired_output'] = f"Error: Blacklist expired script not found at {blacklist_expired_script_path}"
        else:
            try:
                # Execute the script
                cmd = [sys.executable, str(blacklist_expired_script_path)]
                if run_expired_dry_run:
                    cmd.append("--dry-run")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60 # Give it a bit more time
                )
                if result.returncode == 0:
                    st.session_state['blacklist_expired_output'] = result.stdout
                    st.success("Blacklist expired script executed successfully.")
                else:
                    st.session_state['blacklist_expired_output'] = f"Script error (exit {result.returncode}):\n{result.stderr}\n{result.stdout}"
                    st.error("Error executing blacklist expired script.")
            except Exception as e:
                st.session_state['blacklist_expired_output'] = f"Error running script: {e}"
                st.error(f"Error running script: {e}")

    # Display script output if available
    if st.session_state['blacklist_expired_output']:
         st.markdown("#### Script Output:")
         st.code(st.session_state['blacklist_expired_output'])

# Handle tab switching from dashboard buttons
if 'active_tab' in st.session_state:
    active_tab = st.session_state.pop('active_tab', None)
    if active_tab:
        tab_to_switch_to = None
        if active_tab == "WalkScore Enrichment":
            tab_to_switch_to = "WalkScore Enrichment"
        elif active_tab == "Compass Enrichment":
            tab_to_switch_to = "Compass Enrichment"
        elif active_tab == "Cashflow Enrichment":
            tab_to_switch_to = "Cashflow Enrichment"
        
        if tab_to_switch_to:
            st.query_params["tab"] = tab_to_switch_to
            st.session_state['needs_refresh'] = True
            st.rerun()

# Handle refresh after successful operations
if st.session_state.get('needs_refresh', False):
    st.session_state.pop('needs_refresh', None)
    st.rerun()