# resources/database.py

from dagster import resource
import pyodbc

@resource
def database_connection(_):
    return pyodbc.connect("DRIVER={SQL Server};SERVER=server_name;DATABASE=db_name;UID=user;PWD=password")
