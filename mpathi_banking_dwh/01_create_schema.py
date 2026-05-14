import duckdb 
import pandas as pd

#Connect to local duckdb dabase file 
conn = duckdb.connect ('banking_dwh.duckdb')
# Dimension Tables

conn.execute("""
            CREATE TABLE IF NOT EXISTS DIM_CUSTOMER(
            customer_key INTEGER PRIMARY KEY,
            customer_id VARCHAR(20) NOT NULL,
            full_name VARCHAR(100), 
            id_number VARCHAR(20),
            risk_tier VARCHAR(10),  --LOW/MEDIUM/HIGH
            segment VARCHAR(20), --RETAIL/BUSINESS/PREMIUM
            branch_code VARCHAR(10),
            onboard_date DATE, 
            is_active  BOOLEAN   
            )
         """)

conn.execute("""
             CREATE TABLE IF NOT EXISTS DIM_ACCOUNT(
             account_key INTEGER PRIMARY KEY, 
             account_id VARCHAR(20) NOT NULL, 
             account_type VARCHAR(20), --CHEQUE/SAVINGS/CREDIT
             currency VARCHAR(5),
             open_date DATE, 
             status VARCHAR(10), --ACTIVE/DORMANT/CLOSED
             customer_key INTEGER --links back DIM_CUSTOMER
             )
             """)

conn.execute("""
             CREATE TABLE IF NOT EXISTS DIM_BRANCH(
             branch_key INTEGER PRIMARY KEY, 
             branch_code VARCHAR(10) NOT NULL,
             branch_name VARCHAR(100),
             region VARCHAR(50),
             province VARCHAR(50)
             )
             """)

conn.execute("""
            CREATE TABLE IF NOT EXISTS DIM_DATE(
            date_key INTEGER PRIMARY KEY, --format: YYYYMMDD
            full_date DATE, 
            day_of_week VARCHAR(10),
            day_num INTEGER, 
            month_num INTEGER, 
            month_name VARCHAR(10), 
            quater INTEGER, 
            year INTEGER, 
            is_weekend BOOLEAN, 
            is_month_end BOOLEAN
            )          
            """)

conn.execute("""
             CREATE TABLE IF NOT EXISTS DIM_LOAN_PRODUCT(
             loan_product_key INTEGER PRIMARY KEY, 
             product_code VARCHAR(20), 
             product_name VARCHAR(100), 
             product_type VARCHAR(30), --PERSONAL/HOME/VEHICLE?BUSINESS
             interest_rate DECIMAL(5,2),
             max_term_months INTEGER
            )
             """)
# FACT TABLE

conn.execute("""
             CREATE TABLE IF NOT EXISTS FACT_TRANSECTIONS(
             transection_id VARCHAR(30) PRIMARY KEY, 
             date_key INTEGER, --FK-DIM_DATE
             customer_key INTEGER, --FK-DIM_CUSTOMER
             account_key INTEGER, --FK-DIM_ACCOUNT
             branch_key INTEGER, --FK-DIM_BRANCH
             loan_product_key INTEGER, --FK-DIM_LOAN_PRODUCT (nullable)
             transection_type VARCHAR(20), --DEPOSIT/WITHDRAWL/TRANSFER/LOAN_REPAYMENT
             amount DECIMAL(18,2), 
             running_balance DECIMAL(18,2),
             fee_charged DECIMAL(10,2),
             is_flagged BOOLEAN --fraud flag
             )
             """)
print("Schema created successfully.")
conn.close()

import duckdb
conn = duckdb.connect('banking_dwh.duckdb')
tables = conn.execute ("SHOW TABLES").fetchall()
for t in tables: 
    print(t)
