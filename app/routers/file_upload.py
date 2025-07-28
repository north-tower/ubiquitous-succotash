from fastapi import FastAPI, APIRouter, File, UploadFile, HTTPException, Form, BackgroundTasks
from typing import Annotated
import PyPDF2 as py
import io
import tabula
import pandas as pd
from .state import shared_state
from .helpers import get_name_helper
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
from starlette.background import BackgroundTask

router = APIRouter(
    #router tags
    prefix="/file",
    #documentation tags
    tags=['File Upload'],
    responses={
        200: {"description": "Success"},
        400: {"description": "Bad Request"},
        401: {"description": "Unauthorized"},
        404: {"description": "Not Found"},
        500: {"description": "Internal Server Error"}
    }
)

# Add OPTIONS method handler for preflight requests
@router.options("/uploadfileandclean/")
async def options_upload_file():
    return JSONResponse(
        content={"message": "OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
    )

# convert to a df
def dict_to_dataframe(data: dict) -> pd.DataFrame:
    if data is None:
        return {"message": "No data available"}
    try:
        dataframe = pd.DataFrame(data["dataframe"])
        return dataframe
    except KeyError:
        raise ValueError("The provided dictionary does not contain the key 'dataframe'")

mpesa_statement_dictionary = None
mpesa_statement_dataframe = dict_to_dataframe(mpesa_statement_dictionary)

@router.get("/")
def read_root():
    return {"Testing Setup": "File Upload Logic"}

async def process_pdf(pdf_content: bytes, password: str):
    try:
        pdf_file = io.BytesIO(pdf_content)
        pdf_reader = py.PdfReader(pdf_file)

        if pdf_reader.is_encrypted:
            if not pdf_reader.decrypt(password):
                raise HTTPException(status_code=401, detail="Failed to decrypt PDF")

        decrypted_pdf = io.BytesIO()
        pdf_writer = py.PdfWriter()

        for page_num in range(len(pdf_reader.pages)):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        pdf_writer.write(decrypted_pdf)
        decrypted_pdf.seek(0)

        client_details = get_name_helper.extract_client_name(decrypted_pdf)
        dfs = tabula.read_pdf(decrypted_pdf, pages='all')
        
        if not dfs:
            raise HTTPException(status_code=400, detail="No tables found in PDF")

        mpesa_df = pd.concat(dfs, axis=0, ignore_index=True)
        mpesa_df = mpesa_df[mpesa_df['Transaction Status'] == 'Completed']

        # Assign the objects to string values 
        mpesa_df["Paid In"] = mpesa_df['Paid In'].astype(str)
        mpesa_df["Withdrawn"] = mpesa_df['Withdrawn'].astype(str)
        mpesa_df["Balance"] = mpesa_df['Balance'].astype(str)
        mpesa_df["Details"] = mpesa_df["Details"].astype(str)

        # Remove commas from the 'Paid In', 'Withdrawn', and 'Balance' columns before conversion
        mpesa_df["Paid In"] = mpesa_df['Paid In'].str.replace(',', '').astype(float).fillna(0)
        mpesa_df["Withdrawn"] = mpesa_df['Withdrawn'].str.replace(',', '').astype(float).fillna(0)
        mpesa_df["Balance"] = mpesa_df['Balance'].str.replace(',', '').astype(float).fillna(0)
        # Remove '\r' from the 'Name' column
        mpesa_df['Details'] = mpesa_df['Details'].str.replace('\r',' ',regex=False)

        # converting the completion to datetime mpesa_df type 
        mpesa_df['Completion Time'] = pd.to_datetime(mpesa_df['Completion Time'])

        # Dropping columns that have more than 50% missing values
        for column in mpesa_df.columns:
            if mpesa_df[column].isna().sum() / len(mpesa_df) > 0.5:
                mpesa_df.drop(column, axis=1, inplace=True)
            else:
                # Filling remaining missing values with the column mean (for numeric columns only)
                if pd.api.types.is_numeric_dtype(mpesa_df[column]):
                    mpesa_df[column].fillna(mpesa_df[column].mean(), inplace=True)

        # Dropping duplicates
        mpesa_df.drop_duplicates(inplace=True)
        
        # Clean column names by removing any line breaks or unusual characters
        mpesa_df.columns = mpesa_df.columns.str.replace(r'[\r\n]', '', regex=True)

        # Now, drop the column
        mpesa_df.drop(['Transaction Status'], axis=1, inplace=True)  

        # converting the withdrawn mpesa_df into  an abs value 
        mpesa_df["Withdrawn"] = mpesa_df["Withdrawn"].abs()

        # getting the month from the completion time column
        mpesa_df['month_name'] = mpesa_df['Completion Time'].dt.month_name()
        
        # Getting the day name
        mpesa_df['day_name'] = mpesa_df['Completion Time'].dt.day_name()
        # getting the hour the transaction was created
        mpesa_df['Hour']= mpesa_df['Completion Time'].dt.hour

        # Define the mapping dictionary
        mapped_categories = {
            'Customer Transfer to': 'Send Money',
            'Pay Bill Fuliza M-Pesa to' : 'Fuliza Loan',
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
            'Other': 'Other'}
        
        # Initialize the 'Category' column with a default value of 'Other'
        mpesa_df['Category'] = 'Other'

        # Loop through each phrase in mapped_categories and assign the appropriate category
        for phrase, category in mapped_categories.items():
            mpesa_df.loc[mpesa_df['Details'].str.contains(phrase, case=False, na=False), 'Category'] = phrase ##   WHY NOT ASSIGN CATEGORY

        # Map 'Category' to user-friendly names in 'Transaction_Type' column
        mpesa_df['Transaction_Type'] = mpesa_df['Category'].map(mapped_categories)

        mpesa_df.drop(['Category'], axis=1, inplace=True)  
        # Remove rows where 'Transaction_Type' is "Mpesa Charges"
        mpesa_df = mpesa_df.drop(mpesa_df[mpesa_df['Transaction_Type'] == "Mpesa Charges"].index)
        
        # Convert 'Completion Time' to ISO string for JSON serialization
        mpesa_df['Completion Time'] = mpesa_df['Completion Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        print(mpesa_df)

        shared_state.mpesa_statement_df = mpesa_df

        return {
            "client_name": client_details['customer_name'],
            "mobile_number": client_details['mobile_number'],
            "dataframe": mpesa_df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/uploadfileandclean/")
async def upload_file(file: UploadFile, password: Annotated[str, Form()]):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Invalid file type, only PDFs are accepted")
    
    try:
        pdf_content = await file.read()
        result = await process_pdf(pdf_content, password)
        
        return JSONResponse(
            content={
                "filename": file.filename,
                "contenttype": file.content_type,
                **result
            },
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
        )
    except HTTPException as he:
        return JSONResponse(
            status_code=he.status_code,
            content={"detail": he.detail},
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)},
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
        )