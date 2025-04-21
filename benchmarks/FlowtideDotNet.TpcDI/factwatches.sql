
CREATE VIEW watchhistory_read AS
SELECT * FROM watchhistory_raw;


-- temporary parellalism to help with performance
CREATE VIEW watches_aggregate1 AS
SELECT
W_C_ID,
W_S_SYMB,
-- take the latest date a watch was placed, if the same symbol is watched again 
max(W_DTS) FILTER(WHERE W_ACTION = 'ACTV') as date_placed,
max(W_DTS) FILTER(WHERE W_ACTION = 'CNCL') as date_canceled,
min(BatchID) as BatchID
FROM watchhistory_read
WHERE W_C_ID % 2 = 0
GROUP BY W_C_ID, W_S_SYMB;

CREATE VIEW watches_aggregate2 AS
SELECT
W_C_ID,
W_S_SYMB,
-- take the latest date a watch was placed, if the same symbol is watched again 
max(W_DTS) FILTER(WHERE W_ACTION = 'ACTV') as date_placed,
max(W_DTS) FILTER(WHERE W_ACTION = 'CNCL') as date_canceled,
min(BatchID) as BatchID
FROM watchhistory_read
WHERE W_C_ID % 2 = 1
GROUP BY W_C_ID, W_S_SYMB;

CREATE VIEW watches_aggregate AS
SELECT
*
FROM watches_aggregate1
UNION ALL
SELECT
*
FROM watches_aggregate2;

CREATE VIEW watches_single_row AS
SELECT
  W_C_ID,
  W_S_SYMB,
  date_placed,
  date_canceled,
  greatest(date_placed, date_canceled) as dts,
  BatchID
FROM watches_aggregate;


CREATE VIEW FactWatches AS
SELECT
  dc.SK_CustomerID,
  ds.SK_SecurityID,
  CAST(strftime(date_placed, '%Y%m%d') as INT) as SK_DateID_DatePlaced,
  CAST(strftime(date_canceled, '%Y%m%d') as INT) as SK_DateID_DateRemoved,
  w.BatchID
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