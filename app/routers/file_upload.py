from fastapi import FastAPI, APIRouter, File, UploadFile, HTTPException, Form, BackgroundTasks
from typing import Annotated
import PyPDF2 as py
import io
import tabula
import pandas as pd
from .state import shared_state
from .helpers import get_name_helper
import uuid
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
from starlette.background import BackgroundTask
import concurrent.futures
import re

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
@router.options("/uploadfileandclean")
@router.options("/uploadfileandclean/")
async def options_upload_file():
    return JSONResponse(
        content={"message": "OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
            "Content-Type": "application/json",
        }
    )

@router.options("/status/{task_id}")
async def options_status(task_id: str):
    return JSONResponse(
        content={"message": "OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
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

# Task status storage
task_status = {}

# Thread pool for CPU-intensive operations
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

# Cache for category mapping to avoid recreating it
CATEGORY_MAPPING_CACHE = None

@router.get("/")
def read_root():
    return {"Testing Setup": "File Upload Logic"}

def process_pdf_sync(pdf_content: bytes, password: str, progress_callback=None):
    """Synchronous version of process_pdf for thread pool execution"""
    try:
        pdf_file = io.BytesIO(pdf_content)
        pdf_reader = py.PdfReader(pdf_file)

        if pdf_reader.is_encrypted:
            if not pdf_reader.decrypt(password):
                raise HTTPException(status_code=401, detail="Incorrect PDF password. Please check your password and try again.")

        # Create a properly decrypted PDF for both operations
        decrypted_pdf = io.BytesIO()
        pdf_writer = py.PdfWriter()

        for page_num in range(len(pdf_reader.pages)):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        pdf_writer.write(decrypted_pdf)
        decrypted_pdf.seek(0)

        client_details = get_name_helper.extract_client_name(decrypted_pdf)
        if progress_callback:
            progress_callback(30, "Extracting client information...")
            
        # Original simple tabula extraction that was working
        dfs = tabula.read_pdf(decrypted_pdf, pages='all')
        
        if not dfs:
            raise HTTPException(status_code=400, detail="No tables found in PDF")

        if progress_callback:
            progress_callback(40, "Parsing transaction tables...")
            
        mpesa_df = pd.concat(dfs, axis=0, ignore_index=True)
        
        mpesa_df = mpesa_df[mpesa_df['Transaction Status'] == 'Completed']
        
        if progress_callback:
            progress_callback(50, "Filtering completed transactions...")

        # Highly optimized column processing - batch operations
        if progress_callback:
            progress_callback(55, "Optimizing data types...")
        
        # Convert all columns at once for better performance
        string_columns = ["Paid In", "Withdrawn", "Balance", "Details"]
        numeric_columns = ["Paid In", "Withdrawn", "Balance"]
        
        # Batch string conversion
        for col in string_columns:
            if col in mpesa_df.columns:
                mpesa_df[col] = mpesa_df[col].astype('string')  # Use pandas string dtype for better performance
        
        # Batch numeric conversion with optimized operations
        for col in numeric_columns:
            if col in mpesa_df.columns:
                # Use vectorized operations for better performance
                mpesa_df[col] = pd.to_numeric(
                    mpesa_df[col].str.replace(',', '', regex=False), 
                    errors='coerce'
                ).fillna(0)
        
        # Clean Details column
        if 'Details' in mpesa_df.columns:
            mpesa_df['Details'] = mpesa_df['Details'].str.replace('\r', ' ', regex=False)

        if progress_callback:
            progress_callback(60, "Cleaning and formatting data...")

        # Optimized datetime conversion
        mpesa_df['Completion Time'] = pd.to_datetime(mpesa_df['Completion Time'], errors='coerce')

        if progress_callback:
            progress_callback(65, "Processing datetime fields...")

        # Optimized column dropping - vectorized approach
        missing_threshold = len(mpesa_df) * 0.5
        columns_to_drop = []
        numeric_columns_to_fill = []
        
        for column in mpesa_df.columns:
            if mpesa_df[column].isna().sum() > missing_threshold:
                columns_to_drop.append(column)
            elif pd.api.types.is_numeric_dtype(mpesa_df[column]):
                numeric_columns_to_fill.append(column)
        
        # Drop columns and fill numeric columns in one operation
        if columns_to_drop:
            mpesa_df = mpesa_df.drop(columns=columns_to_drop)
        
        if numeric_columns_to_fill:
            mpesa_df[numeric_columns_to_fill] = mpesa_df[numeric_columns_to_fill].fillna(
                mpesa_df[numeric_columns_to_fill].mean()
            )

        if progress_callback:
            progress_callback(70, "Cleaning missing data...")

        # Dropping duplicates
        mpesa_df.drop_duplicates(inplace=True)
        
        # Clean column names by removing any line breaks or unusual characters
        mpesa_df.columns = mpesa_df.columns.str.replace(r'[\r\n]', '', regex=True)

        # Now, drop the column
        mpesa_df.drop(['Transaction Status'], axis=1, inplace=True)  

        if progress_callback:
            progress_callback(72, "Preparing transaction data...")

        # converting the withdrawn mpesa_df into  an abs value 
        mpesa_df["Withdrawn"] = mpesa_df["Withdrawn"].abs()

        # getting the month from the completion time column
        mpesa_df['month_name'] = mpesa_df['Completion Time'].dt.month_name()
        
        # Getting the day name
        mpesa_df['day_name'] = mpesa_df['Completion Time'].dt.day_name()
        # getting the hour the transaction was created
        mpesa_df['Hour']= mpesa_df['Completion Time'].dt.hour

        if progress_callback:
            progress_callback(74, "Extracting time-based features...")

        # Use cached category mapping for better performance
        global CATEGORY_MAPPING_CACHE
        if CATEGORY_MAPPING_CACHE is None:
            CATEGORY_MAPPING_CACHE = {
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
                'Other': 'Other'
            }
        
        mapped_categories = CATEGORY_MAPPING_CACHE
        
        # Category mapping
        mpesa_df['Category'] = 'Other'  # Default value
        
        if progress_callback:
            progress_callback(75, "Starting category mapping...")
        
        # Original category mapping that was working
        for phrase, category in mapped_categories.items():
            mask = mpesa_df['Details'].str.contains(phrase, case=False, na=False)
            mpesa_df.loc[mask, 'Category'] = phrase

        if progress_callback:
            progress_callback(85, "Category mapping completed!")

        # Map 'Category' to user-friendly names in 'Transaction_Type' column
        mpesa_df['Transaction_Type'] = mpesa_df['Category'].map(mapped_categories)

        mpesa_df.drop(['Category'], axis=1, inplace=True)  
        # Remove rows where 'Transaction_Type' is "Mpesa Charges"
        mpesa_df = mpesa_df.drop(mpesa_df[mpesa_df['Transaction_Type'] == "Mpesa Charges"].index)
        
        if progress_callback:
            progress_callback(87, "Filtering transaction types...")
        
        # Convert 'Completion Time' to ISO string for JSON serialization
        mpesa_df['Completion Time'] = mpesa_df['Completion Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

        if progress_callback:
            progress_callback(90, "Preparing final data format...")

        print(mpesa_df)

        shared_state.mpesa_statement_df = mpesa_df

        # Optimize JSON serialization
        if progress_callback:
            progress_callback(95, "Serializing data...")
        
        # Use more efficient serialization
        dataframe_dict = mpesa_df.to_dict(orient="records")
        
        return {
            "client_name": client_details['customer_name'],
            "mobile_number": client_details['mobile_number'],
            "dataframe": dataframe_dict
        }
    except HTTPException:
        # Re-raise HTTP exceptions (like 401 for wrong password)
        raise
    except Exception as e:
        print(f"Error in process_pdf_sync: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        
        # Check if it's a password-related error
        if "PDFPasswordIncorrect" in str(type(e)):
            raise HTTPException(status_code=401, detail="Incorrect PDF password. Please check your password and try again.")
        
        raise HTTPException(status_code=500, detail=str(e))

async def process_pdf_background(pdf_content: bytes, password: str, task_id: str):
    """Background task for processing PDF with detailed progress updates"""
    try:
        print(f"Starting background processing for task {task_id}")
        
        # Initial setup
        task_status[task_id]["progress"] = 5
        task_status[task_id]["status"] = "processing"
        task_status[task_id]["message"] = "Initializing PDF processing..."
        await asyncio.sleep(0.1)
        
        # PDF decryption and reading
        task_status[task_id]["progress"] = 15
        task_status[task_id]["message"] = "Decrypting PDF and extracting content..."
        await asyncio.sleep(0.1)
        
        # Extract tables
        task_status[task_id]["progress"] = 25
        task_status[task_id]["message"] = "Extracting transaction tables from PDF..."
        await asyncio.sleep(0.1)
        
        # Data cleaning and processing
        task_status[task_id]["progress"] = 40
        task_status[task_id]["message"] = "Processing transaction data..."
        await asyncio.sleep(0.1)
        
        # Category mapping
        task_status[task_id]["progress"] = 60
        task_status[task_id]["message"] = "Categorizing transactions..."
        await asyncio.sleep(0.1)
        
        # Final processing
        task_status[task_id]["progress"] = 80
        task_status[task_id]["message"] = "Finalizing data processing..."
        await asyncio.sleep(0.1)
        
        print(f"Processing PDF for task {task_id}")
        
        # Create a progress callback function
        def update_progress(progress_value, message=None):
            task_status[task_id]["progress"] = progress_value
            if message:
                task_status[task_id]["message"] = message
        
        # Run the heavy PDF processing in a thread pool to avoid blocking the server
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(thread_pool, process_pdf_sync, pdf_content, password, update_progress)
        
        # Final steps
        task_status[task_id]["progress"] = 95
        task_status[task_id]["message"] = "Almost done! Finalizing results..."
        await asyncio.sleep(0.1)
        
        task_status[task_id] = {
            "status": "completed",
            "progress": 100,
            "message": "Processing completed successfully!",
            "result": result
        }
        print(f"Background processing completed for task {task_id}")
        
    except Exception as e:
        print(f"Error in background processing for task {task_id}: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        
        # Check if it's a password-related error
        error_message = str(e)
        if "401" in error_message or "PDFPasswordIncorrect" in error_message or "Incorrect PDF password" in error_message:
            task_status[task_id] = {
                "status": "failed",
                "progress": 100,
                "message": "Incorrect PDF password. Please check your password and try again.",
                "error": "Incorrect PDF password"
            }
        else:
            task_status[task_id] = {
                "status": "failed",
                "progress": 100,
                "message": f"Processing failed: {str(e)}",
                "error": str(e)
            }

@router.get("/status/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a background processing task"""
    if task_id not in task_status:
        # Return a default processing status instead of 404
        # This handles race conditions where client polls before task is initialized
        return JSONResponse(
            content={
                "status": "processing",
                "progress": 0,
                "message": "Task is being initialized"
            },
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            }
        )
    
    return JSONResponse(
        content=task_status[task_id],
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )

@router.post("/uploadfileandclean")
@router.post("/uploadfileandclean/")
async def upload_file(file: UploadFile, password: Annotated[str, Form()], background_tasks: BackgroundTasks):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Invalid file type, only PDFs are accepted")
    
    try:
        pdf_content = await file.read()
        
        # Start processing in background
        task_id = str(uuid.uuid4())
        task_status[task_id] = {"status": "processing", "progress": 0}
        background_tasks.add_task(process_pdf_background, pdf_content, password, task_id)
        
        response_data = {
            "message": "File upload accepted, processing started",
            "task_id": task_id,
            "filename": file.filename,
            "status": "processing"
        }
        
        print(f"Returning response: {response_data}")  # Debug log
        
        return response_data
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