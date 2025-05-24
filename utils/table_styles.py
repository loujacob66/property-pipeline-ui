def get_table_styles():
    """Get standardized CSS styles for property tables"""
    return """
    <style>
    /* Property table styles */
    .property-table {
        font-size: 0.9em;
        line-height: 1.2;
    }
    
    /* Column-specific styles */
    .column-address {
        font-weight: 500;
    }
    
    .column-price {
        font-weight: 500;
        color: #0066cc;
    }
    
    .column-status {
        font-weight: 500;
    }
    
    .column-status-active {
        color: #00cc66;
    }
    
    .column-status-pending {
        color: #ff9900;
    }
    
    .column-status-sold {
        color: #cc0000;
    }
    
    /* Interactive table styles */
    .interactive-table {
        font-size: 0.9em;
        line-height: 1.2;
    }
    
    .interactive-table .stDataFrame {
        border: 1px solid #e0e0e0;
        border-radius: 4px;
    }
    
    /* Compact table styles */
    .compact-table {
        font-size: 0.85em;
        line-height: 1.1;
    }
    
    .compact-table .stDataFrame {
        max-height: 400px;
        overflow-y: auto;
    }
    </style>
    """ 