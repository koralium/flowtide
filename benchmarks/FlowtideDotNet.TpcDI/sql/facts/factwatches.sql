
CREATE VIEW watchhistory_read AS
SELECT * FROM watchhistory_raw;

CREATE VIEW watches_window_func_1 AS
SELECT
  W_C_ID,
  W_S_SYMB,
  LAST_VALUE(W_DTS) OVER (PARTITION BY W_C_ID, W_S_SYMB ORDER BY W_DTS DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_placed,
  CASE 
	WHEN W_ACTION = 'CNCL' THEN W_DTS
	ELSE NULL
  END as date_canceled,
  BatchID,
  ROW_NUMBER() OVER (PARTITION BY W_C_ID, W_S_SYMB ORDER BY W_DTS DESC) as rn
FROM watchhistory_read
WHERE W_C_ID % 2 = 0;

CREATE VIEW watches_window_func_2 AS
SELECT
  W_C_ID,
  W_S_SYMB,
  LAST_VALUE(W_DTS) OVER (PARTITION BY W_C_ID, W_S_SYMB ORDER BY W_DTS DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_placed,
  CASE 
	WHEN W_ACTION = 'CNCL' THEN W_DTS
	ELSE NULL
  END as date_canceled,
  BatchID,
  ROW_NUMBER() OVER (PARTITION BY W_C_ID, W_S_SYMB ORDER BY W_DTS DESC) as rn
FROM watchhistory_read
WHERE W_C_ID % 2 = 1;


CREATE VIEW watches_aggregate AS
SELECT
*
FROM watches_window_func_1
WHERE rn = 1
UNION ALL
SELECT
*
FROM watches_window_func_2
WHERE rn = 1;

CREATE VIEW watches_single_row AS
SELECT
  W_C_ID,
  W_S_SYMB,
  date_placed,
  date_canceled,
  greatest(coalesce(date_placed, date_canceled), coalesce(date_canceled, date_placed)) as dts,
  BatchID
FROM watches_aggregate;


CREATE VIEW FactWatches AS
SELECT
  dc.SK_CustomerID,
  ds.SK_SecurityID,
  CAST(strftime(date_placed, '%Y%m%d') as INT) as SK_DateID_DatePlaced,
  CAST(strftime(date_canceled, '%Y%m%d') as INT) as SK_DateID_DateRemoved,
  w.BatchID,
  w.W_C_ID,
  w.dts,
  w.W_S_SYMB,
  dc.CustomerID
FROM watches_single_row w
INNER JOIN DimCustomerView dc
ON w.W_C_ID = dc.CustomerID AND
w.dts >= dc.EffectiveDate AND
w.dts <= dc.EndDate
INNER JOIN DimSecurityView ds
ON w.W_S_SYMB = ds.Symbol AND
w.dts >= ds.EffectiveDate AND
w.dts <= ds.EndDate;

INSERT INTO sink.FactWatches
SELECT * FROM FactWatches;