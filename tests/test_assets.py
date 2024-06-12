# tests/test_assets.py

import pandas as pd
from dagster import build_op_context
from assets.sql_server_assets import raw_table_1

def test_raw_table_1():
    context = build_op_context(partition_key="2023-01-01")
    result = raw_table_1(context)
    assert isinstance(result, pd.DataFrame)
