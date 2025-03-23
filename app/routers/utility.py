from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from .helpers import utility_helper
from .state import shared_state
import re

router = APIRouter(
    #router tags
    prefix="/utility_module",
    #documentation tags
    tags=['Utility Module']
)

@router.get('/data_bills/')
def data_bills():

    data = shared_state.mpesa_statement_df

    # get paybills and tills from statement
    data_utility = data[data["Transaction_Type"].isin(["Pay Bill", "Till No"])]

    data_utility = utility_helper.add_total_amount_column(data_utility)

    # Apply the function conditionally
    data_utility["names"] = data_utility.apply(
        lambda row: utility_helper.get_names(row["Details"]),
        axis=1
    )

    if data_utility is None or data_utility.empty:
        return {"message": "No data bills data"}
    
    data_bills_df = data_utility
    return data_utility.to_dict(orient='records')


@router.get('/kplc/')
# getting the kplc transactions 
def kplc ():
  data_bills_df = pd.DataFrame(data_bills())
  till_numbers_and_paybill = utility_helper.get_paybill_and_till_numbers(data_bills_df, 'Details')
  data_kplc = till_numbers_and_paybill[till_numbers_and_paybill["numbers"].isin(["888888", "888880"])]

  if data_kplc is None or data_kplc.empty:
     return {"message": "No client KPLC data"}
  
  return data_kplc.to_dict(orient='records')


@router.get('/kplc_metrics/')
def utility_analysis():
    # calculating the  metrics 
    data_kplc = kplc()

    # check for error message
    if isinstance(data_kplc, dict) and ("message" in data_kplc or "error" in data_kplc):
        return data_kplc
    
    
    data_kplc = pd.DataFrame(data_kplc)
    transactions_per_month = data_kplc.groupby('month_name').size()
    
    # Calculate the average number of transactions per month
    avg_no_transactions_per_month = transactions_per_month.mean()
    transactions_count =  data_kplc.shape[0]
    transactions_amount = data_kplc["amount"].sum()
    max_transacted = data_kplc["amount"].max()
    min_transacted = data_kplc["amount"].min()
    avg_transacted = data_kplc["amount"].mean()

    return{
        "total_transactions":transactions_count,
        "average_transactions_per_month": avg_no_transactions_per_month,
        "total_tranasacted_amount":transactions_amount,
        "highest_transacted_amount":max_transacted,
        "minimum_transacted_amount":min_transacted,
        "average_transacted_amount":avg_transacted
    }


@router.get('/safaricom_wifi/')
# getting the kplc transactions 
def safaricom_wifi():
    data_bills_df = pd.DataFrame(data_bills())
    till_numbers_and_paybill = utility_helper.get_paybill_and_till_numbers(data_bills_df, 'Details')
    data_safaricom_wifi = till_numbers_and_paybill[till_numbers_and_paybill["numbers"].isin(["150501"])]

    if data_safaricom_wifi is None or data_safaricom_wifi.empty:
        return {"message":"No client Safaricom wifi data"}
    
    return data_safaricom_wifi.to_dict(orient='records')


@router.get('/safaricom_wifi_metrics/')
def utility_analysis():
    # calculating the  metrics 

    data_safaricom_wifi = safaricom_wifi()

    # check for error message
    if isinstance(data_safaricom_wifi, dict) and ("message" in data_safaricom_wifi or "error" in data_safaricom_wifi):
        return data_safaricom_wifi   

    data_safaricom_wifi = pd.DataFrame(data_safaricom_wifi)
    transactions_per_month = data_safaricom_wifi.groupby('month_name').size()
    
    # Calculate the average number of transactions per month
    avg_no_transactions_per_month = transactions_per_month.mean()
    transactions_count =  data_safaricom_wifi.shape[0]
    transactions_amount = data_safaricom_wifi["amount"].sum()
    max_transacted = data_safaricom_wifi["amount"].max()
    min_transacted = data_safaricom_wifi["amount"].min()
    avg_transacted = data_safaricom_wifi["amount"].mean()

    return{
        "total_transactions":transactions_count,
        "average_transactions_per_month": avg_no_transactions_per_month,
        "total_tranasacted_amount":transactions_amount,
        "highest_transacted_amount":max_transacted,
        "minimum_transacted_amount":min_transacted,
        "average_transacted_amount":avg_transacted
    }


@router.get('/zuku_wifi/')
def zuku ():
    data_bills_df = pd.DataFrame(data_bills())
    till_numbers_and_paybill = utility_helper.get_paybill_and_till_numbers(data_bills_df, 'Details')
    data_zuku = till_numbers_and_paybill[till_numbers_and_paybill["numbers"].isin(["320320"])]

    if data_zuku is None or data_zuku.empty:
        return {"message": "No client Zuku data"}
    
    return data_zuku.to_dict(orient="records")


@router.get('/zuku_wifi_metrics/')
def utility_analysis():
    # calculating the  metrics 
    data_zuku_wifi = safaricom_wifi()

    # check for error message
    if isinstance(data_zuku_wifi, dict) and ("message" in data_zuku_wifi or "error" in data_zuku_wifi):
        return data_zuku_wifi   

    data_zuku_wifi - pd.DataFrame(data_zuku_wifi)
    transactions_per_month = data_zuku_wifi.groupby('month_name').size()
    
    # Calculate the average number of transactions per month
    avg_no_transactions_per_month = transactions_per_month.mean()
    transactions_count =  data_zuku_wifi.shape[0]
    transactions_amount = data_zuku_wifi["amount"].sum()
    max_transacted = data_zuku_wifi["amount"].max()
    min_transacted = data_zuku_wifi["amount"].min()
    avg_transacted = data_zuku_wifi["amount"].mean()

    return{
        "total_transactions":transactions_count,
        "average_transactions_per_month": avg_no_transactions_per_month,
        "total_tranasacted_amount":transactions_amount,
        "highest_transacted_amount":max_transacted,
        "minimum_transacted_amount":min_transacted,
        "average_transacted_amount":avg_transacted
    }

@router.get('/fuel/')
def fuel():
    # Use regular expression to filter rows that contain any of the specified names

    data_fuel = data_bills()

    if isinstance(data_fuel, dict) and "message" in data_fuel:
        return {"message": "No fuel data available"}
    
    data_fuel = pd.DataFrame(data_fuel)
    # Filter fuel transactions
    fuel_mask = data_fuel["names"].str.contains(
            "Rubis|Shell|Total|Astrol", 
            case=False, 
            na=False
    )
    data_fuel = data_fuel[fuel_mask]

    if data_fuel is None or data_fuel.empty:
        return {"message":"No client Safaricom wifi data"}

    return data_fuel.to_dict(orient="records")


@router.get('/fuel_metrics/')
def utility_analysis():
    # calculating the  metrics 

    data_fuel = fuel()

    # check for error message
    if isinstance(data_fuel, dict) and ("message" in data_fuel or "error" in data_fuel):
        return data_fuel
      
    data_fuel = pd.DataFrame(data_fuel)
    transactions_per_month = data_fuel.groupby('month_name').size()
    
    # Calculate the average number of transactions per month
    avg_no_transactions_per_month = transactions_per_month.mean()
    transactions_count =  data_fuel.shape[0]
    transactions_amount = data_fuel["amount"].sum()
    max_transacted = data_fuel["amount"].max()
    min_transacted = data_fuel["amount"].min()
    avg_transacted = data_fuel["amount"].mean()

    return{
        "total_transactions":transactions_count,
        "average_transactions_per_month": avg_no_transactions_per_month,
        "total_tranasacted_amount":transactions_amount,
        "highest_transacted_amount":max_transacted,
        "minimum_transacted_amount":min_transacted,
        "average_transacted_amount":avg_transacted
    }