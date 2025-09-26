from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from .state import shared_state
from fastapi.responses import Response, JSONResponse

router = APIRouter(
    #router tags
    prefix="/financial_institutions_module",
    #documentation tags
    tags=['Financial Institutions Module']
)

data = shared_state.mpesa_statement_df

banks_in_kenya = [
    "Kenya Commercial Bank", "KCB", "KCB Bank",
    "Equity Bank Kenya", "Equity Bank", "Equity",
    "Cooperative Bank of Kenya", "Co-op Bank", "Coop Bank",
    "Absa Bank Kenya", "Absa", 
    "Standard Chartered Bank Kenya", "Standard Chartered", "StanChart",
    "NCBA Bank Kenya", "NCBA", 
    "Diamond Trust Bank Kenya", "Diamond Trust Bank", "DTB",
    "I&M Bank Kenya", "I&M Bank", "I&M",
    "Stanbic Bank Kenya", "Stanbic",
    "Family Bank Kenya", "Family Bank", 
    "National Bank of Kenya", "National Bank", "NBK",
    "Bank of Africa Kenya", "Bank of Africa", "BOA",
    "CitiBank Kenya", "CitiBank", "Citi",
    "Housing Finance Group Kenya", "HF Group", "HF",
    "Prime Bank Kenya", "Prime Bank",
    "Spire Bank Kenya", "Spire Bank",
    "Gulf African Bank", "Gulf Bank", 
    "Credit Bank Kenya", "Credit Bank",
    "First Community Bank", "FCB",
    "Victoria Commercial Bank", "Victoria Bank",
    "Consolidated Bank of Kenya", "Consolidated Bank",
    "SBM Bank Kenya", "SBM",
    "Ecobank Kenya", "Ecobank",
    "Guaranty Trust Bank Kenya", "GT Bank", "GTB",
    "Sidian Bank Kenya", "Sidian Bank",
    "Mayfair CIB Bank Kenya", "Mayfair Bank",
    "UBA Kenya Bank", "United Bank for Africa", "UBA",
    "ABC Bank Kenya", "ABC Bank",
    "Transnational Bank Kenya", "Transnational Bank"
]

banks_in_kenya_grouped = {
    "Kenya Commercial Bank": "KCB",
    "KCB": "KCB", 
    "KCB Bank": "KCB",
    "Equity Bank Kenya": "Equity", 
    "Equity Bank": "Equity", 
    "Equity": "Equity",
    "Cooperative Bank of Kenya": "Co-op Bank", 
    "Co-op Bank":  "Co-op Bank", 
    "Coop Bank":  "Co-op Bank",
    "Absa Bank Kenya":  "Absa", 
    "Absa":  "Absa", 
    "Standard Chartered Bank Kenya": "StanChart", 
    "Standard Chartered": "StanChart", 
    "StanChart": "StanChart",
    "NCBA Bank Kenya":   "NCBA", 
    "NCBA":   "NCBA", 
    "Diamond Trust Bank Kenya": "DTB", 
    "Diamond Trust Bank": "DTB", 
    "DTB": "DTB",
    "I&M Bank Kenya":  "I&M", 
    "I&M Bank":  "I&M", 
    "I&M":  "I&M",
    "Stanbic Bank Kenya": "Stanbic", 
    "Stanbic": "Stanbic",
    "Family Bank Kenya": "Family Bank", 
    "Family Bank": "Family Bank", 
    "National Bank of Kenya":  "National Bank", 
    "National Bank":  "National Bank", 
    "NBK":  "National Bank",
    "Bank of Africa Kenya": "Bank of Africa", 
    "Bank of Africa": "Bank of Africa", 
    "BOA": "Bank of Africa",
    "CitiBank Kenya": "CitiBank", 
    "CitiBank": "CitiBank", 
    "Citi": "CitiBank",
    "Housing Finance Group Kenya": "HF Group", 
    "HF Group": "HF Group", 
    "HF": "HF Group",
    "Prime Bank Kenya":  "Prime Bank", 
    "Prime Bank":  "Prime Bank",
    "Spire Bank Kenya": "Spire Bank", 
    "Spire Bank": "Spire Bank",
    "Gulf African Bank":  "Gulf Bank", 
    "Gulf Bank":  "Gulf Bank", 
    "Credit Bank Kenya":  "Credit Bank", 
    "Credit Bank":  "Credit Bank",
    "First Community Bank":   "First Community Bank", 
    "FCB":   "First Community Bank",
    "Victoria Commercial Bank": "Victoria Bank", 
    "Victoria Bank": "Victoria Bank",
    "Consolidated Bank of Kenya": "Consolidated Bank", 
    "Consolidated Bank": "Consolidated Bank",
    "SBM Bank Kenya": "SBM Bank Kenya", 
    "SBM": "SBM Bank Kenya",
    "Ecobank Kenya": "Ecobank", 
    "Ecobank": "Ecobank",
    "Guaranty Trust Bank Kenya": "GT Bank", 
    "GT Bank": "GT Bank", 
    "GTB" : "GT Bank",
    "Sidian Bank Kenya": "Sidian Bank", 
    "Sidian Bank": "Sidian Bank",
    "Mayfair CIB Bank Kenya": "Mayfair Bank", 
    "Mayfair Bank": "Mayfair Bank",
    "UBA Kenya Bank": "UBA Kenya Bank", 
    "United Bank for Africa": "UBA Kenya Bank", 
    "UBA": "UBA Kenya Bank",
    "ABC Bank Kenya": "ABC Bank", 
    "ABC Bank": "ABC Bank",
    "Transnational Bank Kenya": "Transnational Bank", 
    "Transnational Bank": "Transnational Bank"
}

safaricom_financial_services = {
    "M-Shwari":"MShwari", 
    "KCB M-Pesa":"KCB Mpesa",
    "Fuliza" :"M-Pesa Fuliza", 
    "M-Pesa Global" :"Global M-Pesa",
    "H- Fund":"HustlerFund"
}

@router.get("/")
def read_root():
    return JSONResponse(
        content={"Testing Setup": "Financial Institutions Logic"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )

# Helper function for CORS OPTIONS responses
def cors_options_response():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )

# Helper for JSON responses with CORS headers
def cors_json_response(content, status_code: int = 200):
    return JSONResponse(
        content=content,
        status_code=status_code,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )

# Add OPTIONS handlers for CORS preflight requests
@router.options("/")
async def options_root():
    return cors_options_response()

@router.options("/client_banks/")
async def options_client_banks():
    return cors_options_response()

@router.options("/lowest_amount_received_through_bank/")
async def options_lowest_amount_received_through_bank():
    return cors_options_response()

@router.options("/bank_received_summary_metrics/")
async def options_bank_received_summary_metrics():
    return cors_options_response()

@router.options("/lowest_amount_sent_through_bank/")
async def options_lowest_amount_sent_through_bank():
    return cors_options_response()

@router.options("/bank_sent_summary_metrics/")
async def options_bank_sent_summary_metrics():
    return cors_options_response()

@router.options("/identify_safaricom_financial_services/")
async def options_identify_safaricom_financial_services():
    return cors_options_response()

@router.options("/identify_mshwari_financial_transactions/")
async def options_identify_mshwari_financial_transactions():
    return cors_options_response()

@router.options("/mshwari_loan_summary/")
async def options_mshwari_loan_summary():
    return cors_options_response()

@router.options("/top_five_received_count/")
async def options_top_five_received_count():
    return cors_options_response()

@router.options("/top_five_sent_count/")
async def options_top_five_sent_count():
    return cors_options_response()

@router.options("/fuliza_usage/")
async def options_fuliza_usage():
    return cors_options_response()

@router.options("/fuliza_loan_summary/")
async def options_fuliza_loan_summary():
    return cors_options_response()


# Identify Banks Customer Transacts to/from
@router.get('/client_banks/')
def identify_banks():
    try:
        data = shared_state.mpesa_statement_df

        if data is None or data.empty:
            return cors_json_response([[], {"message": "No data available. Please upload a PDF statement first."}])
        
        #create a new column 'Bank' initialized with None
        data['Bank'] = None

        #populate 'Bank' column
        for keyword in banks_in_kenya:
            data.loc[data['Details'].str.contains(keyword, case=False, na=False), 'Bank'] = keyword
    
        #filter rows where 'Bank' is not None
        bank_transactions = data[data['Bank'].notna()]

        unique_banks = bank_transactions['Bank'].unique().tolist()

        return cors_json_response([
            unique_banks,
            {
                "transactions": bank_transactions.to_dict(orient='records'),
                "count": len(bank_transactions)
            }
        ])
    
    except Exception as e:
        print(f"Error identifying banks: {e}")
        return cors_json_response([[], {"message": "Error processing bank data", "error": str(e)}])


# lowest amount received through bank
@router.get('/lowest_amount_received_through_bank/')
def lowest_amount_received_through_bank():
    try:
        _, client_bank_transactions = identify_banks()
        
        if 'transactions' not in client_bank_transactions or not client_bank_transactions['transactions']:
            return cors_json_response({"message": "No bank transaction data available"})
        
        client_bank_transactions = client_bank_transactions['transactions']
        data_df = pd.DataFrame(client_bank_transactions)

        if data_df is None or data_df.empty:
            return cors_json_response({"message": "No data available"})
        
        # to prevent getting zero as the result
        received_df = data_df[data_df['Paid In'] != 0.00]
        
        if received_df.empty:
            return cors_json_response({"message": "No received amounts found"})
        
        lowest_received_amount = received_df['Paid In'].min()
        return cors_json_response({"lowest_received_amount": lowest_received_amount})
    
    except Exception as e:
        print(f"Error getting lowest amount: {e}")
        return cors_json_response({"message": "Error processing bank data", "error": str(e)})


def lowest_amount_received_through_bank(data):

    if data is None:
        return {"error":"No data available"}
    
    try:
        # to prevent getting zero as the result
        received_df = data[data['Paid In'] != 0.00]
        lowest_received_amount = received_df['Paid In'].min()
        return lowest_received_amount
    
    except Exception as e:
        print(f"Error getting lowest amount: {e}")
        return { "error": str(e)}


# bank summary metrics for recieved
@router.get('/bank_received_summary_metrics/')
def bank_received_summary_metrics():
    try:
        _, client_bank_transactions = identify_banks()

        # Check if we got valid data
        if not client_bank_transactions or 'transactions' not in client_bank_transactions:
            return cors_json_response({"message": "No bank transaction data available. Please upload a PDF statement first."})

        client_bank_transactions = client_bank_transactions['transactions']
        
        # Check if transactions list is empty
        if not client_bank_transactions:
            return cors_json_response({"message": "No bank transactions found"})
        
        client_bank_transactions_df = pd.DataFrame(client_bank_transactions)

        # Check if DataFrame is empty
        if client_bank_transactions_df.empty:
            return cors_json_response({"message": "No transaction data available"})

        # debugging
        # print("===============================================")
        # print(f"{client_bank_transactions_df.columns.tolist()}")
        # print("===============================================")

        total_amount_received = client_bank_transactions_df['Paid In'].sum()
        highest_amount_received = client_bank_transactions_df['Paid In'].max()
        lowest_amount_received = lowest_amount_received_through_bank(client_bank_transactions_df)

        highest_amount_bank = client_bank_transactions_df.loc[client_bank_transactions_df['Paid In'].idxmax(), 'Bank']
        lowest_amount_bank = client_bank_transactions_df.loc[client_bank_transactions_df['Paid In'].idxmin(), 'Bank']

        return cors_json_response({
            "total_amount_received": total_amount_received,
            "highest_amount_received": highest_amount_received,
            "lowest_amount_received": lowest_amount_received,
            "highest_amount_bank": highest_amount_bank,
            "lowest_amount_bank":  lowest_amount_bank
        })
    
    except Exception as e:
        print(f"Error in bank_received_summary_metrics: {e}")
        return cors_json_response({"message": "Error processing bank summary metrics", "error": str(e)})


# lowest amount sent through bank
@router.get('/lowest_amount_sent_through_bank/')
def lowest_amount_sent_through_bank():
    try:
        _, client_bank_transactions = identify_banks()
        
        if not client_bank_transactions or 'transactions' not in client_bank_transactions:
            return cors_json_response({"message": "No bank transaction data available. Please upload a PDF statement first."})
        
        client_bank_transactions = client_bank_transactions['transactions']
        
        if not client_bank_transactions:
            return cors_json_response({"message": "No bank transactions found"})
        
        data_df = pd.DataFrame(client_bank_transactions)

        if data_df is None or data_df.empty:
            return cors_json_response({"message": "No data available"})
        
        # to prevent getting zero as the result
        sent_df = data_df[data_df['Withdrawn'] != 0.00]
        
        if sent_df.empty:
            return cors_json_response({"message": "No sent amounts found"})
        
        lowest_sent_amount = sent_df['Withdrawn'].min()
        return cors_json_response({"lowest_sent_amount": lowest_sent_amount})
    
    except Exception as e:
        print(f"Error getting lowest amount sent through bank: {e}")
        return cors_json_response({"message": "Error processing bank data", "error": str(e)})
    

def lowest_amount_sent_through_bank(data):

    if data is None:
        return {"error": "No data available"}
    
    try:
        # to prevent getting zero as the result
        sent_df = data[data['Withdrawn'] != 0.00]
        lowest_sent_amount = sent_df['Withdrawn'].min()
        return {"lowest_sent_amount":  lowest_sent_amount}
    except Exception as e:
        print(f"Error getting lowest amount sent through bank: {e}")
        return {"error": str(e)}


# bank summary metrics for recieved
@router.get('/bank_sent_summary_metrics/')
def bank_sent_summary_metrics():
    _, client_bank_transactions = identify_banks()

    client_bank_transactions = client_bank_transactions['transactions']
    client_bank_transactions_df = pd.DataFrame(client_bank_transactions)

    total_amount_sent = client_bank_transactions_df['Withdrawn'].sum()
    highest_amount_sent = client_bank_transactions_df['Withdrawn'].max()
    lowest_amount_sent = lowest_amount_sent_through_bank(client_bank_transactions_df)

    highest_amount_bank = client_bank_transactions_df.loc[client_bank_transactions_df['Withdrawn'].idxmax(), 'Bank']
    lowest_amount_bank = client_bank_transactions_df.loc[client_bank_transactions_df['Withdrawn'].idxmin(), 'Bank']

    return cors_json_response({
        "total_amount_sent": total_amount_sent,
        "highest_amount_sent": highest_amount_sent,
        "lowest_amount_sent": lowest_amount_sent,
        "highest_amount_bank": highest_amount_bank,
        "lowest_amount_bank":  lowest_amount_bank
    })

# Identify Saf Financial Services/Transactions
@router.get('/identify_safaricom_financial_services/')
def identify_safaricom_financial_services():

    data_df = shared_state.mpesa_statement_df

    #create a new column 'Bank' initialized with None
    data_df['Financial_Service'] = None

    #populate 'Financial_Service' column
    for keyword in safaricom_financial_services:
        data_df.loc[data_df['Details'].str.contains(keyword, case=False, na=False), 'Financial_Service'] = keyword
    
    #filter rows where 'Financial_Service' is not None
    bank_transactions = data_df[data_df['Financial_Service'].notna()]

    return cors_json_response({"transactions": bank_transactions.to_dict(orient='records')})


# M-Shwari
# identify mshwari financial transactions
@router.get('/identify_mshwari_financial_transactions/')
def identify_mshwari_financial_transactions():

    mshwari_financial_services = ["M-Shwari", "MShwari"]
    data_df = shared_state.mpesa_statement_df

    data_df['Mshwari_Service'] = None

    for keyword in mshwari_financial_services:
        data_df.loc[data_df['Details'].str.contains(keyword, case=False, na=False), 'Mshwari_Service'] = keyword
    
    mshwari_transactions = data_df[data_df['Mshwari_Service'].notna()]

    return cors_json_response({"mshwari_transactions": mshwari_transactions.to_dict(orient='records'),
            "count": len(mshwari_transactions)})


def identify_mshwari_financial_transactions_2():

    mshwari_financial_services = ["M-Shwari", "MShwari"]
    data_df = shared_state.mpesa_statement_df

    data_df['Mshwari_Service'] = None

    for keyword in mshwari_financial_services:
        data_df.loc[data_df['Details'].str.contains(keyword, case=False, na=False), 'Mshwari_Service'] = keyword
    
    mshwari_transactions = data_df[data_df['Mshwari_Service'].notna()]

    return cors_json_response({"transactions": mshwari_transactions.to_dict(orient='records')})


# mshwari loan summary
@router.get('/mshwari_loan_summary/')
def mshwari_loan_summary():
    # Identify M-Shwari financial transactions
    mshwari_transactions = identify_mshwari_financial_transactions_2()
    mshwari_transactions = mshwari_transactions['transactions']
    mshwari_transactions_df = pd.DataFrame(mshwari_transactions)

    if mshwari_transactions_df.empty:
        return {"message": "Empty DataFrame"}
    
    # Filter for loan disbursements and repayments
    loan_disbursements = mshwari_transactions_df[mshwari_transactions_df['Transaction_Type'] == 'M-Shwari Loan']
    loan_repayments = mshwari_transactions_df[mshwari_transactions_df['Transaction_Type'] == 'Mshwari Loan Repayment']
    
    # Calculate the required metrics
    total_loan_count = loan_disbursements.shape[0]
    highest_loan_disbursed = loan_disbursements['Paid In'].max() if not loan_disbursements.empty else 0
    highest_loan_paid_back = loan_repayments['Withdrawn'].max() if not loan_repayments.empty else 0
    date_of_last_loan_disbursement = loan_disbursements['Completion Time'].max() if not loan_disbursements.empty else None
    date_of_last_loan_repayment = loan_repayments['Completion Time'].max() if not loan_repayments.empty else None
    last_amount_borrowed = loan_disbursements.loc[loan_disbursements['Completion Time'].idxmax(), 'Paid In'] if not loan_disbursements.empty else 0
    last_amount_paid_back = loan_repayments.loc[loan_repayments['Completion Time'].idxmax(), 'Withdrawn'] if not loan_repayments.empty else 0
    total_loan_disbursed_amount = loan_disbursements['Paid In'].sum() if not loan_disbursements.empty else 0
    total_loan_paid_back_amount = loan_repayments['Withdrawn'].sum() if not loan_repayments.empty else 0
    
    return cors_json_response({
        "total_loan_count": total_loan_count,
        "highest_loan_disbursed": highest_loan_disbursed,
        "highest_loan_paid_back": highest_loan_paid_back,
        "date_of_last_loan_disbursement": date_of_last_loan_disbursement,
        "date_of_last_loan_repayment": date_of_last_loan_repayment,
        "last_amount_borrowed": last_amount_borrowed,
        "last_amount_paid_back": last_amount_paid_back,
        "total_loan_disbursed_amount": total_loan_disbursed_amount,
        "total_loan_paid_back_amount": total_loan_paid_back_amount
    })


def group_bank_mappings(data, mapping):
    # Create a new column 'Grouped_Bank' initialized with None
    data['Grouped_Bank'] = None

    # Populate 'Grouped_Bank' column using the mapping
    for key, value in mapping.items():
        data.loc[data['Details'].str.contains(key, case=False, na=False), 'Grouped_Bank'] = value
    
    # remove rows where 'Grouped_Bank' is None
    data = data.dropna(subset=['Grouped_Bank'])
    
    return data

# top five received (from bank) count
@router.get('/top_five_received_count/')
def top_five_received_count():
    try:
        data_df = shared_state.mpesa_statement_df

        if data_df is None or data_df.empty:
            return cors_json_response({"message": "No data available. Please upload a PDF statement first."})

        df_grouped = group_bank_mappings(data_df, banks_in_kenya_grouped)

        if df_grouped is None:
            return cors_json_response({"message": "No amount received through the bank"})
        
        # filter transactions with Paid In values
        paid_in_bank_transactions = df_grouped[df_grouped['Paid In'] != 0.0]

        # group by 'Grouped Bank' and count number of transactions
        bank_transaction_counts = paid_in_bank_transactions.groupby('Grouped_Bank').size()

        # sort the counts in descending order & select top 5
        top_five_banks = bank_transaction_counts.sort_values(ascending=False).head(5)

        # Convert to dictionary with bank names as keys
        result = {
                "top_five_banks": [
                    {"bank": bank, "count": count} 
                    for bank, count in top_five_banks.items()
                ]
        }

        return cors_json_response(result)
    
    except Exception as e:
        print(f"Error in top_five_received_count: {e}")
        return cors_json_response({"message": "Error processing bank data", "error": str(e)})


# top five sent (from bank) count
@router.get('/top_five_sent_count/')
def top_five_sent_count():
    try:
        data_df = shared_state.mpesa_statement_df

        if data_df is None or data_df.empty:
            return cors_json_response({"message": "No data available. Please upload a PDF statement first."})

        df_grouped = group_bank_mappings(data_df, banks_in_kenya_grouped)

        if df_grouped is None:
            return cors_json_response({"message": "No amount received through the bank"})    
        
        # filter transactions with Withdrawn values
        paid_in_bank_transactions = df_grouped[df_grouped['Withdrawn'] != 0.0]

        # group by 'Grouped Bank' and count number of transactions
        bank_transaction_counts = paid_in_bank_transactions.groupby('Grouped_Bank').size()

        # sort the counts in descending order & select top 5
        top_five_banks = bank_transaction_counts.sort_values(ascending=False).head(5)

        # Convert to dictionary with bank names as keys
        result = {
                "top_five_banks": [
                    {"bank": bank, "count": count} 
                    for bank, count in top_five_banks.items()
                ]
        }

        return cors_json_response(result)
    
    except Exception as e:
        print(f"Error in top_five_sent_count: {e}")
        return cors_json_response({"message": "Error processing bank data", "error": str(e)})


def identify_safaricom_financial_services_2():
    # Get DataFrame
    data_df = shared_state.mpesa_statement_df
    
    if data_df is None or data_df.empty:
        return {"transactions": [], "message": "No data available"}
    
    try:
        # Work with copy
        df = data_df.copy()
        
        # Initialize service column
        df['Financial_Service'] = None
        
        # Map services
        for keyword in safaricom_financial_services:
            mask = df['Details'].str.contains(keyword, case=False, na=False)
            df.loc[mask, 'Financial_Service'] = keyword
        
        # Filter services
        services_df = df[df['Financial_Service'].notna()]
        
        # Return with column info
        return cors_json_response({
            "transactions": services_df.to_dict(orient='records'),
            "columns": services_df.columns.tolist()
        })
    except Exception as e:
        print(f"Error in identify_safaricom_financial_services_2: {e}")
        return cors_json_response({"transactions": [], "error": str(e)})


# fuliza transaction ( How are customers using fuliza)
#How our users are using fuliza 
@router.get('/fuliza_usage/')
def fuliza_usage():
    try:
        #returns banks client uses, and all the bank transactions
        client_saf_transactions = identify_safaricom_financial_services_2()

        if client_saf_transactions is None:
            return cors_json_response({"message": "Client does not use Fuliza"})

        # Check if transactions key exists and has data
        if 'transactions' not in client_saf_transactions or not client_saf_transactions['transactions']:
            return cors_json_response({"message": "No Safaricom financial services data available"})

        data_df = pd.DataFrame(client_saf_transactions['transactions'])
        
        # Check if DataFrame is empty
        if data_df.empty:
            return cors_json_response({"message": "No transaction data available"})
        
        # Check if Financial_Service column exists
        if 'Financial_Service' not in data_df.columns:
            return cors_json_response({"message": "No financial service data available"})
        
        fuliza_data = data_df[data_df["Financial_Service"] == 'Fuliza']

        if fuliza_data.empty:
            return cors_json_response({"message": "No Fuliza usage found"})
        
        return cors_json_response({"fuliza_usage": fuliza_data.to_dict(orient='records')})
    
    except Exception as e:
        print(f"Error in fuliza_usage: {e}")
        return cors_json_response({"message": "Error processing Fuliza usage data", "error": str(e)})


# fuliza loan summary
@router.get('/fuliza_loan_summary/')
def fuliza_loan_summary():
    try:
        data_df = shared_state.mpesa_statement_df
        
        if data_df is None or data_df.empty:
            return cors_json_response({"message": "No transaction data available. Please upload a PDF statement first."})
        
        # Identify M-Shwari financial transactions
        
        # Filter for loan disbursements and repayments
        loan_disbursements = data_df[data_df['Transaction_Type'] == 'Fuliza Loan']
        loan_repayments = data_df[data_df['Transaction_Type'] == 'Fuliza Loan Repayment']
        
        # Calculate the required metrics
        loan_count = len(loan_disbursements)
        highest_amount_paid = loan_repayments["Withdrawn"].max() if not loan_repayments.empty else 0
        highest_amount_disbured = loan_disbursements["Paid In"].max() if not loan_disbursements.empty else 0
        date_of_last_loan_disbursement = loan_disbursements['Completion Time'].max() if not loan_disbursements.empty else None
        date_of_last_loan_repayment = loan_repayments['Completion Time'].max() if not loan_repayments.empty else None
        last_amount_borrowed = loan_disbursements.loc[loan_disbursements['Completion Time'].idxmax(), 'Paid In'] if not loan_disbursements.empty else 0
        last_amount_paid_back = loan_repayments.loc[loan_repayments['Completion Time'].idxmax(), 'Withdrawn'] if not loan_repayments.empty else 0
        total_disbursed = loan_disbursements["Paid In"].sum() if not loan_disbursements.empty else 0
        total_paid = loan_repayments["Withdrawn"].sum() if not loan_repayments.empty else 0
        Total_balance = total_disbursed - total_paid
        
        return cors_json_response({
            "total_loan_count": loan_count,
            "highest_loan_disbursed": highest_amount_disbured,
            "highest_loan_paid_back": highest_amount_paid,
            "date_of_last_loan_disbursement": date_of_last_loan_disbursement,
            "date_of_last_loan_repayment": date_of_last_loan_repayment,
            "last_amount_borrowed": last_amount_borrowed,
            "last_amount_paid_back": last_amount_paid_back,
            "total_loan_disbursed_amount": total_disbursed,
            "total_loan_paid_back_amount": total_paid,
            "total_loan_balance": Total_balance
        })
    
    except Exception as e:
        print(f"Error in fuliza_loan_summary: {e}")
        return cors_json_response({"message": "Error processing Fuliza loan summary", "error": str(e)})
