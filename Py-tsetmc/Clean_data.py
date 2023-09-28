from date_conversion import generate_jalali_date_range
import os
import pandas as pd
from pathlib import Path

import sys
sys.path.append('./Py-tsetmc')

# Read the Excel file
<<<<<<< HEAD
# df = pd.read_excel("./tsetmcdata/14020701.xlsx", skiprows=1, header=1)
=======
df = pd.read_excel("./tsetmcdata/14020701.xlsx", skiprows=1, header=1)
>>>>>>> efbffb527b86a78ed710d40c1f9ff140c4f45e7a

# Cleaning Function


def clean_data(date):
    # Replace "ي" and "" with "ی" and " "respectively:
    date.replace(to_replace=["ي", "ك"], value=[
                 "ی", "ک"], regex=True, inplace=True)
    return date


# cleaned_data = clean_data(all_data)


# Cleaning Function


def clean_combine_data(start_date, end_date):
    # date_list = generate_jalali_date_range(start_date, end_date)
    date_list = [date.replace("-", "") for date in date_list]

    data_dir = list(Path("./tsetmcdata/").glob(f"{date_list}.xlsx"))

    all_data = pd.concat([pd.read_excel(f, skiprows=1, header=1)
                          for f in data_dir], ignore_index=True)
    # Replacing with "ی" and " ک"
    all_data.replace(to_replace=["ي", "ك"], value=[
        "ی", "ک"], regex=True, inplace=True)
    return all_data


clean_combine_data("1401-01-01", "1402-01-01")

# s, e = ("1401-01-01", "1402-01-01")
# date_list = generate_jalali_date_range(s, e)
# date_list=[date.replace("-","") for date in date_list]
# date_list
