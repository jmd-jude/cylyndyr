"""Utilities for data formatting."""
import pandas as pd
import numpy as np
from datetime import datetime

def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent formatting to DataFrame."""
    # Create a copy to avoid modifying the original
    formatted_df = df.copy()
    
    for col in formatted_df.columns:
        # Skip empty columns
        if formatted_df[col].empty:
            continue
        
        # Get first non-null value to check type
        sample_val = formatted_df[col].dropna().iloc[0] if not formatted_df[col].dropna().empty else None
        if sample_val is None:
            continue
        
        col_lower = col.lower()
        
        # Date formatting
        if isinstance(sample_val, (datetime, pd.Timestamp)) or 'date' in col_lower:
            formatted_df[col] = pd.to_datetime(formatted_df[col]).dt.strftime('%Y-%m-%d')
        
        # Numeric formatting
        elif isinstance(sample_val, (int, float, np.number)):
            # Year values - must check first before other numeric formatting
            if ('year' in col_lower or
                (formatted_df[col].between(2010, 2030).all() and 
                formatted_df[col].astype(int).astype(float).eq(formatted_df[col]).all())):
                formatted_df[col] = formatted_df[col].astype(int)
            
            # Currency/Sales formatting - no decimals for large amounts
            elif any(term in col_lower for term in ['sales', 'revenue', 'price', 'amount', 'cost', 'total']):
                formatted_df[col] = formatted_df[col].round(0)
            
            # Large number formatting
            elif formatted_df[col].abs().max() >= 1000:
                formatted_df[col] = formatted_df[col].round(0)
    
    return formatted_df
