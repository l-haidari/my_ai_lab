import numpy as np
import pandas as pd
import google.cloud.bigquery
from google.cloud import bigquery

client = bigquery.Client.from_service_account_json('/home/laleh-haidari/keys/gcloud-uw-bimis-prod.json')
query_new_bill_trials = client.query('WITH cte_date AS(SELECT DISTINCT srvelec_start_at AS date_field FROM `uw-dwh.base.service_electricity_live` )SELECT SAFE_CAST(date_field AS DATE) AS date,COUNT(e.cust_account_number) AS service_live_count FROM cte_date AS d JOIN `uw-dwh.base.service_electricity_live` e ON d.date_field BETWEEN e.srvelec_start_at AND coalesce(e.srvelec_end_at, "2500-01-01") WHERE e.cust_account_number IN (SELECT DISTINCT customer_accountno FROM `uw-net.finance.new_monthly_bill_trail` WHERE trial = 2) AND SAFE_CAST(date_field AS DATE)  <= CURRENT_DATE GROUP BY date_field ORDER BY date_field DESC;')
df_new_bill_trials = query_new_bill_trials.to_dataframe()