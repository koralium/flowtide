CREATE VIEW security_flattened AS
SELECT
floor_timestamp_day(PostingDate) as EffectiveDate,
TRIM(substring(value, 19, 15)) as Symbol,
TRIM(substring(value, 34, 6)) as IssueType,
TRIM(substring(value, 40, 4)) as Status,
TRIM(substring(value, 44, 70)) AS Name,
TRIM(substring(value, 114, 6)) AS ExID,
TRIM(substring(value, 120, 13)) AS ShOut,
TRIM(substring(value, 133, 8)) as FirstTradeDate,
TRIM(substring(value, 141, 8)) as FirstTradeExchg,
TRIM(substring(value, 149, 12)) AS Dividend,
TRIM(substring(value, 161, 60)) AS CoNameOrCIK,
batchId
FROM finwire_data
WHERE RecType = 'SEC';

CREATE VIEW security_with_enddate AS
SELECT
  s.Symbol,
  s.IssueType as Issue,
  s.Status,
  s.Name,
  s.ExID,
  CAST(s.ShOut as INT) as ShOut,
  timestamp_parse(s.FirstTradeDate, 'yyyyMMdd') as FirstTradeDate,
  timestamp_parse(s.FirstTradeExchg, 'yyyyMMdd') AS FirstTradeExchg,
  s.Dividend,
  s.CoNameOrCIK,
  EffectiveDate,
  s.BatchID,
  COALESCE(LEAD(s.EffectiveDate) OVER (PARTITION BY Symbol ORDER BY s.EffectiveDate), CAST('9999-12-31' as TIMESTAMP)) AS EndDate
FROM security_flattened s;

CREATE VIEW security_base AS
SELECT
  s.Symbol,
  s.Issue,
  st.ST_NAME AS Status,
  s.Name,
  s.ExID,
  s.ShOut,
  s.FirstTradeDate,
  s.FirstTradeExchg,
  s.Dividend,
  s.CoNameOrCIK,
  LIST_SORT_ASC_NULL_LAST(
		LIST(
			s.EffectiveDate, 
			GREATEST(s.EffectiveDate, c.EffectiveDate)
		)
	) AS dates,
  s.EffectiveDate,
  s.BatchID,
  c.SK_CompanyID
FROM security_with_enddate s
INNER JOIN StatusTypeView st
ON s.Status = st.ST_ID
INNER JOIN DimCompanyCoNameOrCIK c
ON s.CoNameOrCIK = c.CoNameOrCIK AND
s.EffectiveDate < c.EndDate AND
s.EndDate > c.EffectiveDate;

CREATE VIEW DimSecurityView AS
SELECT 
  surrogate_key_int64() OVER (PARTITION BY Symbol, s.EffectiveDateResolved) as SK_SecurityID,
  Symbol,
  Issue,
  Status,
  Name,
  ExID as ExchangeID,
  s.SK_CompanyID,
  ShOut as SharesOutstanding,
  FirstTradeDate as FirstTrade,
  FirstTradeExchg as FirstTradeOnExchange,
  Dividend,
  LEAD(s.EffectiveDateResolved) OVER (PARTITION BY Symbol ORDER BY s.EffectiveDateResolved) IS NULL AS IsCurrent,
  s.BatchID,
  s.EffectiveDateResolved as EffectiveDate,
  COALESCE(LEAD(s.EffectiveDateResolved) OVER (PARTITION BY Symbol ORDER BY s.EffectiveDateResolved), CAST('9999-12-31' as TIMESTAMP)) AS EndDate
FROM (
	SELECT
	*,
	list_first_difference(dates, LAG(dates) OVER (PARTITION BY Symbol ORDER BY dates)) as EffectiveDateResolved
	FROM
	security_base
) s;

INSERT INTO sink.DimSecurity
SELECT * FROM DimSecurityView;