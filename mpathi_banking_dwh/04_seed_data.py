import duckdb
conn = duckdb.connect('banking_dwh.duckdb')
#TRANCUATE ALL TABLES BEFORE SEEDING
#Safe ro rerun - clears existing data without dropping structure
conn.execute("DELETE FROM FACT_TRANSACTIONS")
conn.execute("DELETE FROM DIM_ACCOUNT")
conn.execute("DELETE FROM DIM_CUSTOMER")
conn.execute("DELETE FROM DIM_LOAN_PRODUCT")
conn.execute("DELETE FROM DIM_DATE")
conn.execute("DELETE FROM DIM_BRANCH")
print("Tables cleared")


import duckdb 
from datetime import date

conn =duckdb.connect('banking_dwh.duckdb')

# DIM_BRANCH
conn.execute("""
INSERT INTO DIM_BRANCH VALUES
(1, 'JHB001', 'Sandton City',      'Johannesburg', 'Gauteng'),
(2, 'JHB002', 'Soweto',            'Johannesburg', 'Gauteng'),
(3, 'PTA001', 'Pretoria Central',  'Pretoria',     'Gauteng'),
(4, 'EKU001', 'Ekurhuleni East',   'Ekurhuleni',   'Gauteng'),
(5, 'JHB003', 'Alexandra',         'Johannesburg', 'Gauteng')          
             """)
#DIM_CUSTOMER
conn.execute("""
INSERT INTO DIM_CUSTOMER VALUES
(1, 'CUS001', 'Thabo Nkosi',       '8001015009087', 'LOW',    'PREMIUM', 'JHB001', '2018-03-15', true),
(2, 'CUS002', 'Priya Naidoo',      '9205120012083', 'MEDIUM', 'RETAIL',  'JHB002', '2020-07-22', true),
(3, 'CUS003', 'Johan van der Berg','7811305009081', 'LOW',    'BUSINESS','PTA001', '2015-11-01', true),
(4, 'CUS004', 'Nomsa Dlamini',     '9912290034089', 'HIGH',   'RETAIL',  'EKU001', '2022-01-10', true),
(5, 'CUS005', 'Sipho Mahlangu',    '8507075009083', 'MEDIUM', 'RETAIL',  'JHB003', '2019-06-30', false)
             """)            
#DIM_ACCOUNT
conn.execute ("""
INSERT INTO DIM_ACCOUNT VALUES
(1, 'ACC001', 'CHEQUE',  'ZAR', '2018-03-15', 'ACTIVE',  1),
(2, 'ACC002', 'SAVINGS', 'ZAR', '2020-07-22', 'ACTIVE',  2),
(3, 'ACC003', 'CHEQUE',  'ZAR', '2015-11-01', 'ACTIVE',  3),
(4, 'ACC004', 'CREDIT',  'ZAR', '2022-01-10', 'DORMANT', 4),
(5, 'ACC005', 'SAVINGS', 'ZAR', '2019-06-30', 'CLOSED',  5)             
              """)
#DIM_LOAN_PRODUCT
conn.execute ("""
INSERT INTO DIM_LOAN_PRODUCT VALUES
(1, 'PL001', 'Personal Loan Standard', 'PERSONAL', 12.50, 60),
(2, 'HL001', 'Home Loan Prime',        'HOME',     10.25, 240),
(3, 'VL001', 'Vehicle Finance',        'VEHICLE',  11.75, 72),
(4, 'BL001', 'Business Term Loan',     'BUSINESS',  9.50, 120)
              """)
#DIM_DATE
conn.execute ("""
INSERT INTO DIM_DATE VALUES
(20240101, '2024-01-01', 'Monday',    1,  1,  'January',  1, 2024, false, false),
(20240131, '2024-01-31', 'Wednesday', 31, 1,  'January',  1, 2024, false, true),
(20240229, '2024-02-29', 'Thursday',  29, 2,  'February', 1, 2024, false, true),
(20240315, '2024-03-15', 'Friday',    15, 3,  'March',    1, 2024, false, false),
(20240401, '2024-04-01', 'Monday',    1,  4,  'April',    2, 2024, false, false),
(20240630, '2024-06-30', 'Sunday',    30, 6,  'June',     2, 2024, true,  true),
(20240915, '2024-09-15', 'Sunday',    15, 9,  'September',3, 2024, true,  false),
(20241201, '2024-12-01', 'Sunday',    1,  12, 'December', 4, 2024, true,  false),
(20241231, '2024-12-31', 'Tuesday',   31, 12, 'December', 4, 2024, false, true)             
              """)
#FACT_TRANSACTIONS
conn.execute ("""
INSERT INTO FACT_TRANSACTIONS VALUES
('TXN001', 20240101, 1, 1, 1, NULL, 'DEPOSIT',        15000.00, 15000.00, 0.00,  false),
('TXN002', 20240131, 2, 2, 2, NULL, 'WITHDRAWAL',      2500.00,  8500.00, 5.50,  false),
('TXN003', 20240229, 3, 3, 3, 1,    'LOAN_REPAYMENT',  3200.00, 42000.00, 0.00,  false),
('TXN004', 20240315, 4, 4, 4, NULL, 'DEPOSIT',          500.00,   500.00, 0.00,  false),
('TXN005', 20240401, 1, 1, 1, NULL, 'WITHDRAWAL',      5000.00, 10000.00, 5.50,  false),
('TXN006', 20240630, 2, 2, 2, NULL, 'DEPOSIT',         12000.00, 20500.00, 0.00, false),
('TXN007', 20240915, 3, 3, 3, 2,    'LOAN_REPAYMENT',  8500.00, 33500.00, 0.00,  false),
('TXN008', 20241201, 4, 4, 4, NULL, 'TRANSFER',        1000.00,  1500.00, 12.00, true),
('TXN009', 20241231, 1, 1, 1, NULL, 'DEPOSIT',        20000.00, 30000.00, 0.00,  false),
('TXN010', 20240131, 5, 5, 5, 3,    'LOAN_REPAYMENT',  4100.00,  2000.00, 0.00,  false)
              """)
print("Seed data loaded successfully.")
print(f"Branches: {conn.execute('SELECT COUNT(*) FROM DIM_BRANCH').fetchone()[0]}")
print(f"Customers: {conn.execute('SELECT COUNT(*) FROM DIM_CUSTOMER').fetchone()[0]}")
print(f"Accounts: {conn.execute('SELECT COUNT(*) FROM DIM_ACCOUNT').fetchone()[0]}")
print(f"Loan Products: {conn.execute('SELECT COUNT(*) FROM DIM_LOAN_PRODUCT').fetchone()[0]}")
print(f"Dates: {conn.execute('SELECT COUNT(*) FROM DIM_DATE').fetchone()[0]}")
print(f"Transactions: {conn.execute('SELECT COUNT(*) FROM FACT_TRANSACTIONS').fetchone()[0]}")

conn.close()
