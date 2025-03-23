from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from . import file_upload
from .state import shared_state
from .helpers import transactions_helper

router = APIRouter(
    #router tags
    prefix="/transaction_module",
    #documentation tags
    tags=['Transaction Module']
)

data = shared_state.mpesa_statement_df

@router.get("/")
def read_root():
    return {"Testing Setup": "Transaction Module Logic"}

# number of transactions per type per amount
@router.get("/trans_type/")
def trans_type():
    data = shared_state.mpesa_statement_df
    data = transactions_helper.add_total_amount_column(data)

    if data is None or data.empty:
        return {"message" : "No transaction data available"}

    # Group data by 'Transaction_Type', aggregate count and sum of 'Amount'
    types = data.groupby("Transaction_Type").agg(
        Count=("Transaction_Type", "count"),  # Count occurrences of each transaction type
        Total_Amount=("amount", "sum")      # Sum amounts for each transaction type
    )
    return types.to_dict(orient='records')

# total amount transacted

# total amount transacted - received
@router.get("/total_recieved/")
def total_received():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    total = data['Paid In'].sum()

    return {"total": total}


# total amount transacted - withdrawn
@router.get('/total_withdrawn/')
def total_withrdrawn():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    total = data['Withdrawn'].sum()
    return {"total":total}

# total withdrawn plus total received
@router.get('/total_transacted/')
def total_transacted():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    total = total_received(data) + total_withrdrawn(data)
    return {"total":total}


# total number of transactions

# total withdrawals count
@router.get('/withdrawal_count/')
def number_of_withdrawals():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    withdrawal_df = data[data['Withdrawn'] != 0.00]
    num_of_withdrawals = len(withdrawal_df)
    return {"no_of_withdrawals":num_of_withdrawals}


# total deposits count
@router.get('/deposit_count/')
def number_of_deposits():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    deposits_df = data[data['Paid In'] != 0.00]
    num_of_deposits = len(deposits_df)
    return {"number_of_deposits":num_of_deposits}


# withdrawal count plus deposit count
@router.get('total_transaction_count')
def total_number_of_transactions():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    total = number_of_withdrawals(data) + number_of_deposits(data)
    return {"total_no_of_transactions":total}


# top deposit
@router.get('/top_deposit/')
def highest_received():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    highest_received_amount = data['Paid In'].max()
    return {"highest_receoved_amount":highest_received_amount}


# lowest deposit
@router.get('/lowest_deposit/')
def lowest_received():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    # to prevent getting zero as the result
    received_df = data[data['Paid In'] != 0.00]
    lowest_received_amount = received_df['Paid In'].min()
    return {"lowest_amount_received":lowest_received_amount}


# top withdrawal
@router.get('/top_withdrawal/')
def highest_withdrawn():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    highest_withdrawn_amount = data['Withdrawn'].max()
    return {"highest_withdrawn_amount":highest_withdrawn_amount}


# lowest withdrawal
@router.get('/lowest_withdrawal/')
def lowest_withdrawn():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    # to prevent getting zero as the result
    withdrawal_df = data[data['Withdrawn'] != 0.00]
    lowest_withdrawn_amount = withdrawal_df['Withdrawn'].min()
    return {"lowest_withdrawn_amount":lowest_withdrawn_amount}


# minimum amount transacted
@router.get('/minimum_amount_transacted/')
def min_amount_transacted():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    lowest_deposit_amount = lowest_received(data)
    lowest_withdrawn_amount = lowest_withdrawn(data)

    if lowest_withdrawn_amount < lowest_deposit_amount:
        return lowest_withdrawn_amount
    else:
        return {"lowest_deposit_amount":lowest_deposit_amount}
    

# maximum amount transacted
@router.get('/maximum_amount_transacted/')
def max_amount_transacted(data):
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    highest_deposit_amount = highest_received(data)
    highest_withdrawn_amount = highest_withdrawn(data)

    if highest_withdrawn_amount > highest_deposit_amount:
        return highest_withdrawn_amount
    else:
        return {"highest_deposit_amount": highest_deposit_amount}


# top paybill transactions
@router.get('/top_paybill_transactions/')
def top_transactions():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    # Filter rows where Transaction_Type is "Pay Bill"
    data = data[data['Transaction_Type'] == "Pay Bill"]
    
    # Group by 'names' and 'numbers' and aggregate
    data_grouped = data.groupby(['names', 'numbers']).agg(
        receipt_count=('Receipt No.', 'count'),  # Count of 'Receipt No.'
        max_amount=('amount', 'max')  # Max of 'amount'
    ).reset_index()  # Reset index after aggregation
    
    # Get the top 10 rows based on receipt_count
    data_final = data_grouped.nlargest(10, 'receipt_count')
    
    return {"data_final": data_final}


# top till transactions
@router.get('/top_till_transactions/')
def top_transactions_till ():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    data = data[data['Transaction_Type'] == "Till No"]
    # group the data
    data = data.groupby(["names","numbers"])
    # aggregate the data 
    data =data.agg({'Receipt No.': 'count', 'amount': 'sum'})
    # reset the index

    # Get the top 10 rows based on the 'amount' column
    data = data.nlargest(10, 'Receipt No.')

    return {"data": data}


# top send money transactions
@router.get('/top_send_money_transactions/')
def top_transactions_send_money ():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    data = data[data['Transaction_Type'] == "Send Money"]
    # group the data
    data = data.groupby(["names","numbers"])
    # aggregate the data 
    data =data.agg({'Receipt No.': 'count', 'amount': 'sum'})
    # reset the index

    # Get the top 10 rows based on the 'amount' column
    data = data.nlargest(10, 'Receipt No.')

    return {"data":data}


# top transaction customer
@router.get('/top_transactions_customer/')
def top_transactions_customer ():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    data = data[data['Transaction_Type'] == "Customer Deposit"]
    # group the data
    data = data.groupby(["names","numbers"])
    # aggregate the data 
    data =data.agg({'Receipt No.': 'count', 'amount': 'sum'})
    # reset the index
    data = data.reset_index()
    # Get the top 10 rows based on the 'amount' column
    data = data.nlargest(10, 'Receipt No.')

    return {"data":data}


## top 10 withdrawals
@router.get('/top_withdrawals/')
def top_transactions_withrawals ():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    data = data[data['Transaction_Type'] == "Cash Withdrawal"]
    # group the data
    data_group= data.groupby(["names","numbers"])
    # aggregate the data 
    data_agg=data_group.agg({'Receipt No.': 'count', 'amount': 'sum'})
    # reset the index
    data_res= data_agg.reset_index()
    # Get the top 10 rows based on the 'amount' column
    data_final = data_res.nlargest(10, 'Receipt No.')

    return {"data_final":data_final}


@router.get('/top_transactions_received/')
def top_transactions_recieved():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    data = data[data['Transaction_Type'] == "Received Money"]
    # group the data
    data_group= data.groupby(["names","numbers"])
    # aggregate the data 
    data_agg=data_group.agg({'Receipt No.': 'count', 'amount': 'sum'})
    # reset the index
    data_res= data_agg.reset_index()
    # Get the top 10 rows based on the 'amount' column
    data_final = data_res.nlargest(10, 'Receipt No.')

    return {"data_final":data_final}


	
## Getting the  number of transactions transacted per day (time of day)
@router.get('/top_transaction_hour/')
def top_transactions_hour():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    # group the data
    data_group= data.groupby(["time_day"])
    # aggregate the data 
    data_agg=data_group.agg({'Receipt No.': 'count', 'amount': 'mean'})
    # reset the index
    data_res= data_agg.reset_index()
    # Get the top 10 rows based on the 'amount' column
    data_final = data_res.nlargest(10, 'Receipt No.')

    return {"data_final":data_final}


# getting the transactions distributed per week
@router.get('/top_transaction_day/')
def top_transactions_day():
    data = shared_state.mpesa_statement_df

    if data is None or data.empty:
        return {"message" : "No transaction data available"}
    
    # group the data
    data_group= data.groupby(["day_name"])
    # aggregate the data 
    data_agg=data_group.agg({'Receipt No.': 'count', 'amount': 'mean'})
    # reset the index
    data_res= data_agg.reset_index()
    # Get the top 10 rows based on the 'amount' column
    data_final = data_res.nlargest(10, 'Receipt No.')

    return {"data_final":data_final}
