# Fraud-Detection-project-
Pandas and SQL project 
```sql
SELECT * FROM synthetic_fraud_dataset;

ALTER TABLE synthetic_fraud_dataset RENAME TO fraud_detection;
```

```sql
SELECT * 
FROM fraud_detection
LIMIT 1; 
```
```sql
CREATE TABLE final_fraud AS 
SELECT 
c1 AS transaction_id, 
c2 AS user_id, 
c3 AS amount, 
c4 AS transaction_type, 
c5 AS merchant_category, 
c6 AS country, 
c7 AS hour, 
c8 AS device_risk_score, 
c9 AS ip_risk_score, 
c10 AS is_fraud
FROM fraud_detection
Where c1 <> 'Name'; 
```

```sql
SELECT * 
FROM final_fraud; 

DELETE FROM final_fraud
WHERE transaction_id = 'transaction_id';
```
```sql
-- No NULLs in key columns 
SELECT * 
FROM final_fraud 
WHERE transaction_id IS NULL
	OR user_id IS NULL 
    OR amount IS NULL; 
```

```sql
-- Distribution of fraud vs legitimate 
SELECT is_fraud, COUNT(*) As transaction_count
FROM final_fraud
GROUP BY is_fraud; 
```

```sql
-- Fraud by country and hour 
SELECT country, hour, SUM(is_fraud) AS fraud_count
FROm final_fraud 
GROUP BY country, hour; 
```

```sql
-- Top 10 users with highest fraud activity 

SELECT user_id, SUM(is_fraud) AS fraud_count, COUNT(*) AS total_transactions 
FROM final_fraud 
GROUP BY user_id 
ORDER BY fraud_count
LIMIT 10; 
```

```sql
--High risk transactions based on risk scores 
SELECT *, 
	CASE 
    	WHEN device_risk_score > 7 OR ip_risk_score > 7 THEN 'High Risk' 
        ELSE 'Low Risk' 
       END AS risk_category 
   FROM final_fraud; 
```

```sql
 -- fraud rate by hour 
 SELECT hour, 
 	COUNT(*) AS total_transactions, 
    SUM(is_fraud) AS fraud_transactions, 
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
  FROM final_fraud
  GROUP BY hour 
  ORDER BY fraud_rate; 
  ```

```sql
  -- Users with abnormal fraud frequency 
  SELECT user_id, 
  	COUNT(*) AS total_transactions, 
    SUM(is_fraud) AS fraud_transactions, 
    ROUND(SUM(is_fraud) *1.0 / COUNT(*), 2) AS fraud_ratio
  FROM final_fraud 
  GROUP BY user_id
  HAVING COUNT(*) > 5
  ORDER BY fraud_ratio DESC; 
```

```sql
--  Repeat fraud device patterns 
SELECt device_risk_score, 
COUNT(*) AS fraud_count 
FROM final_fraud
WHERE is_fraud = 1 
GROUP BY device_risk_score
ORDER BY fraud_count DESC; 
```

```sql
-- Rolling fraud trend 
SELECT transaction_id, 
user_id, 
SUM(is_fraud) OVER (PARTITION BY user_id ORDER BY transaction_id) AS rank 
FROM final_fraud; 
```

```sql
-- Stored procedure 
CREATE OR REPLACE PROCEDURE run_fraud_pipeline()
RETURNS STRING 
LANGUAGE SQL 
AS 
$$
BEGIN 
	CREATE OR REPlACE TABLE fraud_hourly_summary AS 
    SELECT 
    	hour, 
        COUNT(*) AS total_transactions, 
        SUM(is_fraud) AS fraud_transactins, 
        ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate 
    FROM final_fraud
    GROUP BY hour; 
    
    CREATE OR REPLACE TABLE fraud_country_summary AS 
    SELECT 
    	country, 
        COUNT(*) AS total_transactions, 
        SUM(is_fraud) AS fraud_transactions
    FROM final_fraud
    GROUP BY country; 

        CREATE OR REPLACE TABLE fraud_user_risk AS
    SELECT
        user_id,
        COUNT(*) AS total_txns,
        SUM(is_fraud) AS fraud_cases
    FROM final_fraud
    GROUP BY user_id;

    RETURN 'Fraud pipeline executed successfully';

END;
$$;

CALL run_fraud_pipeline();
```
