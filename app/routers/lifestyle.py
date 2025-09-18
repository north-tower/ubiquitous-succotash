from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from . import file_upload
import re
from .state import shared_state
import numpy as np
from .helpers import lifestyle_helper
import logging
from fastapi.responses import Response

router = APIRouter(
    #router tags
    prefix="/lifestyle_module",
    #documentation tags
    tags=['Lifestyle Module']
)

# Add OPTIONS handlers for CORS preflight requests
@router.options("/betting_summary_stats/")
async def options_betting_summary_stats():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )

@router.options("/saving_summary_stats/")
async def options_saving_summary_stats():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )

@router.options("/shopping_summary_stats/")
async def options_shopping_summary_stats():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Max-Age": "3600",
        }
    )


#A function to get the betting summary statistics
@router.get('/betting_summary_stats/')
def betting_summary_stats():
    try:
        data_df = shared_state.mpesa_statement_df
        
        if data_df is None or data_df.empty:
            return {"message": "No transaction data available"}
        
        # calculating the metrics
        gambling_data = lifestyle_helper.get_gambling_df(data_df)

        # Check if get_gambling_df returned None or empty data
        if gambling_data is None or gambling_data.empty:
            return {"message": "No gambling data found"}
        
        # Check if required columns exist
        if 'month_name' not in gambling_data.columns:
            return {"message": "Missing month_name column in gambling data"}
        
        if 'amount' not in gambling_data.columns:
            return {"message": "Missing amount column in gambling data"}
        
        transactions_per_month = gambling_data.groupby('month_name').size()

        # Calculate the average number of transactions per month
        avg_no_transactions_per_month = transactions_per_month.mean()
        transactions_count = gambling_data.shape[0]
        transactions_amount = gambling_data["amount"].sum()
        max_transacted = gambling_data["amount"].max()
        min_transacted = gambling_data["amount"].min()
        avg_transacted = gambling_data["amount"].mean()

        return {
            "total_transactions": str(transactions_count),
            "average_transactions_per_month": str(avg_no_transactions_per_month),
            "total_tranasacted_amount": str(transactions_amount),
            "highest_transacted_amount": str(max_transacted),
            "minimum_transacted_amount": str(min_transacted),
            "average_transacted_amount": str(avg_transacted)
        }
        
    except Exception as e:
        logging.error(f"Error in betting_summary_stats: {e}")
        return {"error": str(e)}


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
        # Get shopping data
        data = lifestyle_helper.get_supermarket_df()
        
        if data is None or data.empty:
            return {"message": "No shopping transactions found"}
            
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