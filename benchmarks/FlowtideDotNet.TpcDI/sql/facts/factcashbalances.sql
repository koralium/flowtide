
CREATE VIEW cashtransactions_per_day AS
SELECT
  CT_CA_ID as AccountID,
  floor_timestamp_day(CT_DTS) as TransactionDate,
  SUM(CT_AMT) as Cash,
  MIN(BatchID) as BatchID
FROM cashtransaction_raw
GROUP BY
  CT_CA_ID,
  floor_timestamp_day(CT_DTS);

CREATE VIEW cashbalances AS
SELECT
  AccountID,
  SUM(Cash) OVER (PARTITION BY AccountID ORDER BY TransactionDate) AS Cash,
  TransactionDate,
  BatchID
FROM cashtransactions_per_day;

CREATE VIEW FactCashBalancesView AS
SELECT
  da.SK_CustomerID,
  da.SK_AccountID,
  CAST(timestamp_format(cb.TransactionDate, 'yyyyMMdd') as INT) as SK_DateID,
  cb.Cash,
  cb.BatchID
FROM cashbalances cb
INNER JOIN DimAccountView da
ON cb.AccountID = da.AccountID AND
cb.TransactionDate >= da.EffectiveDate AND
cb.TransactionDate <= da.EndDate;

INSERT INTO sink.FactCashBalances
SELECT * FROM FactCashBalancesView;