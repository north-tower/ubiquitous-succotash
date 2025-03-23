from pandas import DataFrame

class SharedState:
    mpesa_statement_df: DataFrame | None = None

shared_state = SharedState()