from fastapi import FastAPI, APIRouter, HTTPException
from typing import Annotated
import pandas as pd
from . import file_upload
import re
from .state import shared_state
import numpy as np
from .helpers import credit_score_helper
import logging

router = APIRouter(
    #router tags
    prefix="/credit_score_module",
    #documentation tags
    tags=['Credit Score Module']
)

data = shared_state.mpesa_statement_df

@router.get("/get_credit_score")
def get_credit_score():
    data = shared_state.mpesa_statement_df

    data["save/spend"] = data.apply(credit_score_helper.save_or_spend, axis=1)

    credit_score = credit_score_helper.calculate_mpesa_fico_score(data)

    credit_score_status = credit_score_helper.credit_score_status(credit_score)

    return {
                "credit_score": credit_score,
                "credit_score_status": credit_score_status
            }