
CREATE VIEW customer_account_events AS
SELECT 
* 
FROM CustomerMgmt c
WHERE c.ActionType IN ('NEW', 'ADDACCT', 'UPDACCT', 'CLOSEACCT');

CREATE VIEW accounts_all_events AS
SELECT 
  c.ActionType,
  CASE 
    WHEN c.ActionType IN ('NEW', 'ADDACCT', 'UPDACCT') THEN 'ACTIVE'
    ELSE 'INACTIVE'
  END as Status,
  acc.CA_ID as AccountID, 
  CAST(c.Customer.C_ID AS INT) as CustomerID,
  CAST(acc.CA_B_ID AS INT) as BrokerID,
  acc.CA_TAX_ST as TaxStatus, 
  acc.CA_NAME as AccountDesc,
  c.ActionTS,
  floor_timestamp_day(c.ActionTS) AS EffectiveDate,
  1 as BatchID
FROM customer_account_events c
JOIN UNNEST(c.Customer.Account) acc;

-- This view contains all account updates and resolves to their latest values
CREATE VIEW accounts_resolved AS
SELECT
  AccountID,
  CustomerID,
  Status,
  LAST_VALUE(TaxStatus) IGNORE NULLS OVER (PARTITION BY AccountID ORDER BY ActionTS) as TaxStatus,
  LAST_VALUE(BrokerID) IGNORE NULLS OVER (PARTITION BY AccountID ORDER BY ActionTS) as BrokerID,
  LAST_VALUE(AccountDesc) IGNORE NULLS OVER (PARTITION BY AccountID ORDER BY ActionTS) as AccountDesc,
  EffectiveDate,
  BatchID,
  ActionTS
FROM accounts_all_events;

-- Create one row per day with the latest values, also find the end date of each row used for joins
CREATE VIEW accounts_per_day AS
SELECT
  AccountID,
  CustomerID,
  Status,
  TaxStatus,
  BrokerID,
  AccountDesc,
  EffectiveDate,
  COALESCE(LEAD(EffectiveDate) OVER (PARTITION BY AccountID ORDER BY EffectiveDate), CAST('9999-12-31' as TIMESTAMP)) AS EndDate,
  BatchID
FROM accounts_resolved
WHERE ROW_NUMBER() OVER (PARTITION BY AccountID, EffectiveDate ORDER BY ActionTS DESC) = 1;

-- Contains accounts joined with other dimensions, and a dates list of all the effective dates in sorted order
CREATE VIEW accounts_base AS
SELECT
  a.AccountID,
  a.CustomerID,
  CASE 
	WHEN c.Status = 'ACTIVE' AND a.Status = 'ACTIVE' THEN 'ACTIVE'
	ELSE 'INACTIVE'
  END as Status,
  a.TaxStatus,
  a.BrokerID,
  a.AccountDesc,
  a.EffectiveDate,
  a.BatchID,
  LIST_SORT_ASC_NULL_LAST(
		LIST(
			a.EffectiveDate, 
			GREATEST(a.EffectiveDate, b.EffectiveDate),
			GREATEST(a.EffectiveDate, c.EffectiveDate)
		)
	) AS dates,
  b.SK_BrokerID,
  c.SK_CustomerID
FROM accounts_per_day a
INNER JOIN DimBrokerView b 
ON 
	b.BrokerID = a.BrokerID AND 
	b.EffectiveDate <= a.EndDate AND 
	b.endDate >= a.EffectiveDate
INNER JOIN DimCustomerView c
ON 
	c.CustomerID = a.CustomerID AND
	c.EffectiveDate <= a.EndDate AND
	c.endDate >= a.EffectiveDate;


CREATE VIEW DimAccountView AS
SELECT
  surrogate_key_int64() OVER (PARTITION BY AccountID, a.EffectiveDateResolved) as SK_AccountID,
  AccountID,
  SK_BrokerID,
  SK_CustomerID,
  Status,
  AccountDesc,
  TaxStatus,
  LEAD(a.EffectiveDateResolved) OVER (PARTITION BY AccountID ORDER BY a.EffectiveDateResolved) IS NULL AS IsCurrent,
  BatchID,
  a.EffectiveDateResolved as EffectiveDate,
  COALESCE(LEAD(EffectiveDateResolved) OVER (PARTITION BY AccountID ORDER BY a.EffectiveDateResolved), CAST('9999-12-31' as TIMESTAMP)) AS EndDate
FROM (
	SELECT
	*,
	list_first_difference(dates, LAG(dates) OVER (PARTITION BY AccountID ORDER BY dates)) as EffectiveDateResolved
	FROM
	accounts_base
) a;

INSERT INTO sink.DimAccount
SELECT * FROM DimAccountView;