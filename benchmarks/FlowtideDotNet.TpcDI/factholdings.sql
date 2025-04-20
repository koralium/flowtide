CREATE VIEW FactHoldingsView AS
SELECT
  HH_H_T_ID as TradeID,
  HH_T_ID as CurrentTradeID,
  dt.SK_CustomerID,
  dt.SK_AccountID,
  dt.SK_SecurityID,
  dt.SK_CompanyID,
  dt.SK_CloseDateID as SK_DateID,
  dt.SK_CloseTimeID as SK_TimeID,
  dt.TradePrice as CurrentPrice,
  HH_AFTER_QTY as CurrentHolding,
  1 as BatchID
FROM holdinghistory_raw hh
INNER JOIN DimTradeView dt
ON hh.HH_T_ID = dt.TradeID;


INSERT INTO blackhole
SELECT * FROM FactHoldingsView;

