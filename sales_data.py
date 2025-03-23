import pandas as pd
import numpy as np

def extract(file):
    df = pd.read_csv(file)
    return df

raw_data = extract('ETL PROJECTS/sales_data.csv') 

def transform(raw_data):
    raw_data['date'] = pd.to_datetime(raw_data['date'])
    raw_data['category'] = raw_data['category'].str.upper()
    raw_data['category'] = raw_data['category'].str.strip()
    return raw_data

clean_data = transform(raw_data)

total_sales = clean_data.groupby('category')['total_price'].sum()

top_sales = clean_data.groupby('category')['quantity'].sum().sort_values(ascending=False).head(5)
print(top_sales)    

total_sales.to_csv('total_sales.csv')
top_sales.to_csv('top_sales.csv') 
