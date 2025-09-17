from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel
from app.routers.file_upload import router as file_upload
from app.routers.transactions import router as transactions
from app.routers.financial_institutions import router as financial_institutions
from app.routers.lifestyle import router as lifestyle
from app.routers.utility import router as utility
from app.routers.credit_score import router as credit_score
from fastapi.middleware import cors
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

# Define allowed origins - replace with your actual frontend URL
origins = [
    "http://localhost:3000",  # React default
    "http://localhost:5173",  # Vite default
    "http://127.0.0.1:3000",  # Local development
    "http://127.0.0.1:5173",  # Local development
    "http://164.68.115.204",  # Your server IP
    "http://164.68.115.204:8000",
    "http://164.68.115.204:3000",
    "http://164.68.115.204:5173",
    "*",  # Allow all origins for testing
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
    expose_headers=["*"],  # Expose all headers
    max_age=3600,  # Cache preflight requests for 1 hour
)

app.include_router(file_upload)
app.include_router(financial_institutions)
app.include_router(lifestyle)
app.include_router(utility)
app.include_router(transactions)
app.include_router(credit_score)

class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}