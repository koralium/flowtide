
CREATE VIEW tradehistory_joined AS
SELECT
  T_ID as TradeID,
  th.TH_ST_ID as StatusID,
  T_TT_ID as TradeTypeID,
  TH_DTS AS DTS,
  CASE WHEN T_IS_CASH = '1' THEN true ELSE false END as IsCash,
  T_S_SYMB as Symbol,
  T_QTY as Quantity,
  T_BID_PRICE as BidPrice,
  CAST(T_CA_ID as INT) as CustomerAccountId,
  T_EXEC_NAME as ExecName,
  T_TRADE_PRICE as TradePrice,
  T_CHRG as Charge,
  T_COMM as Commision,
  T_TAX as Tax,
  1 as BatchID
FROM trade_batch1_raw t
INNER JOIN tradehistory_raw th
ON t.T_ID = th.TH_T_ID;

CREATE VIEW trade_incremental AS
SELECT
T_ID as TradeID,
  T_ST_ID as StatusID,
  T_TT_ID as TradeTypeID,
  T_DTS AS DTS,
  CASE WHEN T_IS_CASH = '1' THEN true ELSE false END as IsCash,
  T_S_SYMB as Symbol,
  T_QTY as Quantity,
  T_BID_PRICE as BidPrice,
  CAST(T_CA_ID as INT) as CustomerAccountId,
  T_EXEC_NAME as ExecName,
  T_TRADE_PRICE as TradePrice,
  T_CHRG as Charge,
  T_COMM as Commision,
  T_TAX as Tax,
  BatchID
FROM trade_incremental_raw;

CREATE VIEW trade_all AS
SELECT * FROM tradehistory_joined
UNION ALL
SELECT * FROM trade_incremental;

CREATE VIEW trade_latest AS
SELECT
  TradeID,
  -- Get the latest state of each trade
  MAX_BY(named_struct(
      'StatusID', StatusID,
      'TradeTypeID', TradeTypeID,
      'IsCash', IsCash,
      'Symbol', Symbol,
      'Quantity', Quantity,
      'BidPrice', BidPrice,
      'CustomerAccountId', CustomerAccountId,
      'ExecName', ExecName,
      'TradePrice', TradePrice,
      'Charge', Charge,
      'Commision', Commision,
      'Tax', Tax
    ), DTS) AS latest_state,
  MIN(DTS) FILTER (
      WHERE (StatusID = 'SBMT' AND TradeTypeID IN ('TMB', 'TMS'))
         OR StatusID = 'PNDG'
    ) AS RawCreateDTS,
  MIN(DTS) FILTER (
      WHERE StatusID IN ('CMPT', 'CNCL')
    ) AS RawCloseDTS,
  -- Find the first occurance for PTS (used to join dim tables) and batchId since it should be the first occurance
  MIN_BY(named_struct('DTS', DTS, 'BatchID', BatchID), DTS) as FirstOccurance
FROM trade_all
GROUP BY TradeID;

CREATE VIEW DimTradeView AS
SELECT
  TradeID,
  da.SK_BrokerID,
  CAST(strftime(RawCreateDTS, '%Y%m%d') as INT) as SK_CreateDateID,
  CAST(strftime(RawCreateDTS, '%H%M%S') as INT) as SK_CreateTimeID,
  CAST(strftime(RawCloseDTS, '%Y%m%d') as INT) as SK_CloseDateID,
  CAST(strftime(RawCloseDTS, '%H%M%S') as INT) as SK_CloseTimeID,
  st.ST_NAME as Status,
  tt.TT_NAME as Type,
  latest_state.IsCash as IsCashFlag,
  ds.SK_SecurityID as SK_SecurityID,
  ds.SK_CompanyID as SK_CompanyID,
  latest_state.Quantity,
  latest_state.BidPrice,
  da.SK_AccountID as SK_AccountID,
  da.SK_CustomerID as SK_CustomerID,
  latest_state.ExecName as ExecutedBy,
  latest_state.TradePrice,
  CHECK_VALUE(
    latest_state.Charge,
    latest_state.Charge is null OR (latest_state.Charge < (latest_state.Quantity * latest_state.BidPrice)),
    'T_ID = {TradeID}, T_CHRG = {Charge}',
    TradeID,
    latest_state.Charge,
    messageText => 'Invalid trade fee',
    MessageSource => 'DimTrade'
  ) as Fee,
  CHECK_VALUE(
    latest_state.Commision,
    latest_state.Commision is null OR (latest_state.Commision < (latest_state.Quantity * latest_state.BidPrice)),
    'T_ID = {TradeID}, T_COMM = {Commision}',
    TradeID,
    latest_state.Commision,
    messageText => 'Invalid trade commission',
    MessageSource => 'DimTrade'
  ) AS Commision,
  latest_state.Tax,
  FirstOccurance.BatchID
FROM trade_latest tl
INNER JOIN StatusTypeView st
ON latest_state.StatusID = st.ST_ID
INNER JOIN TradeTypeView tt
ON latest_state.TradeTypeID = tt.TT_ID
INNER JOIN DimSecurityView ds
ON latest_state.Symbol = ds.Symbol AND
FirstOccurance.DTS >= ds.EffectiveDate AND
FirstOccurance.DTS <= ds.EndDate
INNER JOIN DimAccountView da
ON latest_state.CustomerAccountId = da.AccountID AND
FirstOccurance.DTS >= da.EffectiveDate AND
FirstOccurance.DTS <= da.EndDate;

INSERT INTO sink.DimTrade
SELECT * FROM DimTradeView;