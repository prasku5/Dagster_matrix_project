# partitions/monthly_partitions.py

from dagster import MonthlyPartitionsDefinition
from dagster import DailyPartitionsDefinition
from dagster import WeeklyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(start_date="2023-01-01") # Define monthly partitions
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01") # Define daily partitions
weekly_partitions = WeeklyPartitionsDefinition(start_date="2023-01-01") # Define weekly partitions

