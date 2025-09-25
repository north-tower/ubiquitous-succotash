from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.file_upload import router as file_upload
from app.routers.transactions import router as transactions
from app.routers.financial_institutions import router as financial_institutions
from app.routers.lifestyle import router as lifestyle
from app.routers.utility import router as utility
from app.routers.credit_score import router as credit_score


app = FastAPI()

# Explicitly allow only your frontend URLs
origins = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://164.68.115.204:3000",   # React frontend (server)
    "http://164.68.115.204:5173",   # Vite frontend (server)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        # Explicit origins
    allow_credentials=True,       # Needed if you use cookies or Authorization headers
    allow_methods=["*"],          # Allow all HTTP methods
    allow_headers=["*"],          # Allow all headers
    expose_headers=["*"],         # Optional: expose extra headers
    max_age=3600,                 # Cache preflight request for 1 hour
)

# Routers
app.include_router(file_upload)
app.include_router(financial_institutions)
app.include_router(lifestyle)
app.include_router(utility)
app.include_router(transactions)
app.include_router(credit_score)


@app.get("/")
def read_root():
    return {"Hello": "World"}
