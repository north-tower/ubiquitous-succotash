import pandas as pd
import numpy as np
from ..state import shared_state
import re
import logging
logging.basicConfig(level=logging.INFO)

data = shared_state.mpesa_statement_df

def date_columns(data):
    """A function that gets the month and day of the week from the columns"""
    # getting the month from the completion time column
    data['month_name'] = data['Completion Time'].dt.month_name()
    
    # Getting the day name
    data['day_name'] = data['Completion Time'].dt.day_name()
    # getting the hour the transaction was created
    data['Hour']= data['Completion Time'].dt.hour


    return data


def map_financial_transactions_categories(data:pd.DataFrame | None):
    """Categorize 'Details' column entries and map them to cleaned, user-friendly categories."""
    
    if data is None:
        return 
    
    if data.empty:
        return

    # Define the mapping dictionary
    mapped_categories = {
        'Customer Transfer to': 'Send Money',
        'Pay Bill Fuliza M-Pesa to' : 'Pay Bill',
        'Customer Transfer Fuliza MPesa': 'Send Money',
        'Pay Bill Online': 'Pay Bill',
        'Pay Bill to': 'Pay Bill',
        'Customer Transfer of Funds Charge': 'Mpesa Charges',
        'Pay Bill Charge': 'Mpesa Charges',
        'Merchant Payment Online': 'Till No',
        'Customer Send Money to Micro': 'Pochi',
        'M-Shwari Withdraw': 'Mshwari Withdraw',
        'Business Payment from': 'Bank Transfer',
        'Airtime Purchase': 'Airtime Purchase',
        'Airtime Purchase For Other': 'Airtime Purchase',
        'Recharge for Customer': 'safaricom bundles',
        'Customer Bundle Purchase with Fuliza': 'safaricom bundles',
        'Funds received from': 'Received Money',
        'Merchant Payment': 'Till No',
        'Customer Withdrawal': 'Cash Withdrawal',
        'Withdrawal Charge': 'Mpesa Charges',
        'Pay Merchant Charge': 'Mpesa Charges',
        'M-Shwari Deposit': 'Mshwari Deposit',
        'M-Shwari Loan': 'M-Shwari Loan',
        'M-Shwari Loan Repayment':'M-Shwari Repayment',
        'Deposit of Funds at Agent': 'Customer Deposit',
        'OD Loan Repayment to': 'Fuliza Loan Repayment',
        'OverDraft of Credit Party': 'Fuliza Loan',
        'Customer Transfer Fuliza M-Pesa to':'Send Money',
        'Customer Transfer of Funds Charge':'Mpesa Charges',
        'KCB M-PESA Withdraw': 'KCB M-PESA Withdraw',
        'KCB M-PESA Deposit': 'KCB M-PESA Deposit',
        'KCB M-PESA Target Deposit': 'KCB M-PESA Deposit',	
        'Recharge for Customer With Fuliza': 'Fuliza Airtime',
        'Promotion Payment':'Received Money',
        'KCB M-PESA Target First Deposit':'KCB M-PESA Deposit',
        'Customer Payment to Small Business':'Pochi',
        'Merchant Customer Payment from': 'Till No',
        'Reversal':'Reversal',
        'Merchant Payment Fuliza M-Pesa':'Till No',
        'H-fund':'Hustler',
        'Other': 'Other'}
    
    try:
    
        # Initialize the 'Category' column with a default value of 'Other'
        data['Category'] = 'Other'

        # Loop through each phrase in mapped_categories and assign the appropriate category
        for phrase, category in mapped_categories.items():
            data.loc[data['Details'].str.contains(phrase, case=False, na=False), 'Category'] = phrase

        # Map 'Category' to user-friendly names in 'Transaction_Type' column
        data['Transaction_Type'] = data['Category'].map(mapped_categories)

        return data
    
    except Exception as e:
        print(f"Error processing data: {e}")
        return 


def drop_unwanted_rows(data: pd.DataFrame):
    """A function that drops unwanted rows  that were created during mapping of the  categories"""    # Now, drop the column
    try:
        # check if DataFrame is None or empty
        if data is None or data.empty:
            return
        
        # check if 'Category' column exists before dropping
        if 'Category' in data.columns:
            data.drop(['Category'], axis=1, inplace=True) 
        

        # Remove rows where 'Transaction_Type' is "Mpesa Charges"
        data = data.drop(data[data['Transaction_Type'] == "Mpesa Charges"].index)

        return data

    except Exception as e:
        print(f"Error dropping rows: {e}") 

        return


def add_total_amount_column(data: pd.DataFrame) -> pd.DataFrame:
    """Add total amount column"""
    if 'Withdrawn' not in data.columns or 'Paid In' not in data.columns:
        raise ValueError("Required columns 'Withdrawn' and 'Paid In' not found")
    
    # Handle missing values
    data['Withdrawn'] = data['Withdrawn'].fillna(0)
    data['Paid In'] = data['Paid In'].fillna(0)
    
    # Add total amount column
    data['amount'] = data['Withdrawn'] + data['Paid In']
    
    # Replace NaN and infinite values with None
    data.replace([np.inf, -np.inf, np.nan], None, inplace=True)
    
    return data


def extract_names(details):
    "A function to extract individual names from the details column"
    # Regex to capture names after a number and/or special characters, followed by a space
    match = re.search(r"\d[\d\*\#\&\-]*\s([a-zA-Z\s\-]+)", details)
    if match:
        name = match.group(1).strip()
        # Condition: Add only if the name has more than 3 characters
        if len(name) > 3:
            return name
    return details # Return None if no valid name is found


def extract_numbers(data, details_col):
   
    # Define the primary pattern for numbers after 'to' and before '-'
    pattern_to = r"to\s+(\d+)\s*-\s*"
    # Define the secondary pattern for masked numbers
    pattern_masked = r"[0-9]\*{6}\d{3}|[0-9]"

    def extract_number(row):
        text = str(row).strip()
        # Try to match the primary pattern
        match = re.search(pattern_to, text)
        if match:
            return match.group(1)
        # If no match, try the secondary pattern
        masked_match = re.findall(pattern_masked, text)
        return "".join(masked_match) if masked_match else None

    
    data["numbers"] = data[details_col].apply(extract_number)

    return data


# empty dataframe - to be used if 'df_cleaned' is empty or not there
initial_df = pd.DataFrame(columns=['Details','Transaction_Type', 'Category'])


# savings
def get_saving_df():
    """Get savings related transactions"""
    try:
        # Get data from shared state
        data = shared_state.mpesa_statement_df
        
        if data is None or data.empty:
            logging.warning("No data available in shared state")
            return None
            
        # Filter savings transactions
        savings_pattern = r'M-Shwari Lock Activate|SANLAM|M-Shwari Deposit'
        data_saving = data[data["Details"].str.contains(savings_pattern, case=False, regex=True)]
        
        if data_saving.empty:
            logging.info("No savings transactions found")
            return None
            
        return data_saving
        
    except Exception as e:
        logging.error(f"Error in get_saving_df: {e}")
        return None


# shopping
def get_supermarket_df():

    try:
        data = shared_state.mpesa_statement_df

        if data is None or data.empty:
            return {"message":"No data"}
        
        # Use regular expression to filter rows that contain any of the specified names
        data_super = data[data["names"].str.contains("Quick Mart|Naivas|Tuskys", case=False, na=False)]

        if data is None or data.empty:
            return {"message":"No supermarket data"}

        return data_super 

    except Exception as e:
        logging.error(f"Error in get_supermarket_df: {e}")
        return None

# betting
def get_gambling_df(data_df: pd.DataFrame) -> pd.DataFrame:

    logging.info(f"Initial data shape: {data_df.shape if data_df is not None else 'None'}")

    data_df = date_columns(data_df)

    data_df = map_financial_transactions_categories(data_df)

    if data_df is None or data_df.empty:
        logging.warning("No data in shared state")
        return {"error":"Empty get_gambling df"}
    
    data_df = drop_unwanted_rows(data_df)

    data_df = add_total_amount_column(data_df)

    if data_df is not None:
        
        data_df["names"] = data_df.apply(
                lambda row: extract_names(row["Details"]),
                axis=1
        )
        
        data_df = extract_numbers(data_df, "Details")
        logging.info(f"Final data shape: {data_df.shape}")

    print(data_df.to_dict(orient='records'))
    # Convert numbers to integers
    data_df["numbers"] = pd.to_numeric(data_df["numbers"], errors='coerce')
    data_df["numbers"] = data_df["numbers"].fillna(0).astype(int)
    
    
    # Debug data types and content
    logging.info("DataFrame Info:")
    logging.info(data_df.info())
        
    logging.info("\nNumbers column data type:")
    logging.info(data_df["numbers"].dtype)
        
    logging.info("\nSample of numbers column:")
    logging.info(data_df["numbers"].head())
    

    data_betting = data_df[data_df["numbers"].isin([int(4097371),int(290290),int(290680),int(955100)])]
    logging.info(f"Final data shape: {data_betting.shape}")
    print(data_betting.to_dict(orient="records"))

    return data_betting