from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from . import file_upload
import re
from .state import shared_state
import numpy as np
from .helpers import lifestyle_helper
import logging

router = APIRouter(
    #router tags
    prefix="/lifestyle_module",
    #documentation tags
    tags=['Lifestyle Module']
)


#A function to get the betting summary statistics
@router.get('/betting_summary_stats/')
def betting_summary_stats():

    data_df = shared_state.mpesa_statement_df
    # calculating the  metrics
    data_df = lifestyle_helper.get_gambling_df(data_df)

    data = data_df

    if data is None:
        return {"message": "No gambling data"}
    
    transactions_per_month = data.groupby('month_name').size()

    
    # Calculate the average number of transactions per month
    avg_no_transactions_per_month = transactions_per_month.mean()
    transactions_count =  data.shape[0]
    transactions_amount = data["amount"].sum()
    max_transacted = data["amount"].max()
    min_transacted = data["amount"].min()
    avg_transacted = data["amount"].mean()

    return{
        "total_transactions": str(transactions_count),
        "average_transactions_per_month": str(avg_no_transactions_per_month),
        "total_tranasacted_amount":str(transactions_amount),
        "highest_transacted_amount":str(max_transacted),
        "minimum_transacted_amount":str(min_transacted),
        "average_transacted_amount":str(avg_transacted)
    }


#A function to get the saving summary statistics
@router.get('/saving_summary_stats/')
def savings_analysis():
    try:
        # Get savings data
        data = lifestyle_helper.get_saving_df()
        
        if data is None or data.empty:
            return {"message": "No savings transactions found"}
            
        # Calculate metrics
        transactions_per_month = data.groupby('month_name').size()
        
        return {
            "total_transactions": int(data.shape[0]),
            "average_transactions_per_month": float(transactions_per_month.mean()),
            "total_tranasacted_amount": float(data["amount"].sum()),
            "highest_transacted_amount": float(data["amount"].max()),
            "minimum_transacted_amount": float(data["amount"].min()),
            "average_transacted_amount": float(data["amount"].mean())
        }
        
    except Exception as e:
        logging.error(f"Error analyzing savings: {e}")
        return {"error": str(e)}


#A function to get the shopping summary statistics
@router.get('/shopping_summary_stats/')
def shopping_summary_analysis():
    try:
        # Get savings data
        data = lifestyle_helper.get_supermarket_df()
        
        if data is None or data.empty:
            return {"message": "No savings transactions found"}
            
        # Calculate metrics
        transactions_per_month = data.groupby('month_name').size()
        
        return {
            "total_transactions": int(data.shape[0]),
            "average_transactions_per_month": float(transactions_per_month.mean()),
            "total_tranasacted_amount": float(data["amount"].sum()),
            "highest_transacted_amount": float(data["amount"].max()),
            "minimum_transacted_amount": float(data["amount"].min()),
            "average_transacted_amount": float(data["amount"].mean())
        }
        
    except Exception as e:
        logging.error(f"Error analyzing savings: {e}")
        return {"error": str(e)}