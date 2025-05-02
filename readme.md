# Property Pipeline UI

A Streamlit-based user interface for managing and visualizing real estate property data from the property-pipeline project.

## Overview

This UI provides a comprehensive dashboard for interacting with your property listings database and running data enrichment scripts. It includes:

- Dashboard with key metrics and visualizations
- Property explorer with filtering and sorting capabilities
- Data enrichment controls for running various scripts
- Advanced analytics for investment analysis and comparison

## Installation

### Prerequisites

- Python 3.7 or higher
- Streamlit (`pip install streamlit`)
- Pandas (`pip install pandas`)
- Plotly (`pip install plotly`)
- SQLite3 (included with Python)

### Setup

1. Clone this repository next to your property-pipeline project:

```
/your-folder/
  /property-pipeline/    # Your existing project
  /property-pipeline-ui/ # This UI project
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Start the Streamlit app:

```bash
cd property-pipeline-ui
streamlit run app.py
```

## Usage

The application has four main sections:

### 1. Dashboard

The dashboard displays summary statistics and key metrics for your property listings, including:
- Total properties count
- Average price, square footage, and price per square foot
- Property distribution by city, price range, and property type
- Recent listings overview

### 2. Property Explorer

The explorer allows you to:
- Filter properties by various criteria (price, beds, baths, location, etc.)
- View properties in table and map formats
- Visualize property data through charts and graphs
- Export filtered data as CSV or Excel

### 3. Data Enrichment

This section provides controls to run your property-pipeline scripts:
- Gmail Parser: Extract property listings from emails
- Compass Enrichment: Add MLS numbers, tax information, and other details
- WalkScore Enrichment: Add Walk Score, Transit Score, and Bike Score information

Each script can be configured with parameters before execution, and log output is displayed.

### 4. Analytics

The analytics section offers in-depth analysis of your property data:
- Investment Analysis: Rent yield, price trends, and top investment opportunities
- Location Analysis: City comparison, WalkScore analysis, and geographic trends
- Property Comparison: Property types, configurations (beds/baths), and price per square foot analysis
- Data Export: Customizable data export for further analysis

## Configuration

On startup, the app will try to automatically locate your property-pipeline project and database. You can adjust these paths in the sidebar if needed:

- **Database Path**: Path to your listings.db SQLite database
- **Scripts Path**: Path to the folder containing your Python scripts

## Troubleshooting

- **Database Connection Error**: Ensure your database exists at the specified path. You can initialize the database from the main page if needed.
- **Script Execution Error**: Check that the script paths are correct in the sidebar configuration.
- **Missing Data**: Some visualizations require specific data fields. Run the appropriate enrichment scripts to populate missing data.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
