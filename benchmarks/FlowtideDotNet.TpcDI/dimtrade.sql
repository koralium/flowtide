
CREATE VIEW tradehistory_base AS
SELECT
  T_ID as TradeID,
  th.TH_ST_ID as StatusID,
  T_TT_ID as TradeTypeID,
  TH_DTS AS DTS,
  CASE WHEN T_IS_CASH = '1' THEN true ELSE false END as IsCash,
  T_S_SYMB as Symbol,
  T_QTY as Quantity,
  T_BID_PRICE as BidPrice,
  T_CA_ID as CustomerAccountId,
  T_EXEC_NAME as ExecName,
  T_TRADE_PRICE as TradePrice,
  T_CHRG as Charge,
  T_COMM as Commision,
  T_TAX as Tax
FROM trade_raw t
INNER JOIN tradehistory_raw th
ON t.T_ID = th.TH_T_ID;

INSERT INTO console
SELECT * FROM tradehistory_base;



--  INSERT INTO blackhole
--  SELECT
--	tradeid,
--	create_ts,
--	max_time,
--	status
--	FROM tradehistory_base
--	WHERE create_ts != max_time;