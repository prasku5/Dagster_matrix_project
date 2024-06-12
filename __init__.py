# __init__.py

from dagster import Definitions
from assets.sql_server_assets import raw_table_1, raw_table_2, raw_table_3
from assets.s3_assets import s3_file_1, s3_file_2, s3_file_3
from assets.transformations import transformed_table_1, transformed_table_2, transformed_table_3
from schedules.daily_schedule import daily_schedule
from schedules.weekly_schedule import weekly_schedule
from sensors.s3_sensor import s3_sensor
from resources.database import database_connection
from resources.s3 import s3_client

defs = Definitions(
    assets=[
        s3_file_1,
        s3_file_2,
        s3_file_3,
        raw_table_1,
        raw_table_2,
        raw_table_3,
        raw_weekly_table_1,
        transformed_table_1,
        transformed_weekly_table_1,
    ],
    schedules=[
        daily_schedule,
        weekly_schedule,
    ],
    sensors=[
        s3_sensor,
    ],
    resources={
        "database_connection": database_connection,
        "s3_client": s3_client,
    },
)
