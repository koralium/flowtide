CREATE VIEW earnings_per_year_company AS
SELECT
  fv.SK_CompanyID,
  FI_QTR_START_DATE,
  sum(FI_BASIC_EPS) OVER (PARTITION BY dc.CompanyID ORDER BY FI_QTR_START_DATE ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) YEAR_FI_BASIC_EPS
FROM FinancialView fv
INNER JOIN DimCompanyView dc
ON fv.SK_CompanyID = dc.SK_CompanyID;

CREATE VIEW markethistory_base AS
SELECT
  dm.DM_S_SYMB as DM_S_SYMB,
  DM_CLOSE as ClosePrice,
  DM_HIGH as DayHigh,
  DM_LOW as DayLow,
  DM_VOL as Volume,
  DM_DATE as DM_DATE,
  min_by(
	  named_struct(
		  'low', dm.DM_LOW, 
		  'date', dm.DM_DATE
	  ), DM_LOW) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearLow,
  MAX_BY(named_struct(
		  'high', dm.DM_HIGH, 
		  'date', dm.DM_DATE
	  ), DM_HIGH) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearHigh
FROM dailymarket_raw dm;

CREATE VIEW FactMarketHistoryView AS
SELECT
  ds.SK_SecurityID,
  ds.SK_CompanyID,
  CAST(strftime(mh.DM_DATE, '%Y%m%d') as INT) as SK_DateID,
  mh.ClosePrice / YEAR_FI_BASIC_EPS as PERatio,
  mh.DM_S_SYMB,
  mh.YearHigh.high as FiftyTwoWeekHigh,
  CAST(strftime(mh.YearHigh.date, '%Y%m%d') as INT) as SK_FiftyTwoWeekHighDate,
  mh.YearLow.low as FiftyTwoWeekLow,
  CAST(strftime(mh.YearLow.date, '%Y%m%d') as INT) as SK_FiftyTwoWeekLowDate,
  ClosePrice,
  DayHigh,
  DayLow,
  Volume
FROM markethistory_base mh
INNER JOIN DimSecurityView ds
ON ds.Symbol = mh.DM_S_SYMB AND
mh.DM_DATE >= ds.EffectiveDate AND
mh.DM_DATE <= ds.EndDate
LEFT JOIN earnings_per_year_company epyc
ON epyc.SK_CompanyID = ds.SK_CompanyID AND
timestamp_extract('QUARTER', mh.DM_DATE) = timestamp_extract('QUARTER', epyc.FI_QTR_START_DATE) AND
timestamp_extract('YEAR', mh.DM_DATE) = timestamp_extract('YEAR', epyc.FI_QTR_START_DATE);

INSERT INTO sink.FactMarketHistory
SELECT * FROM FactMarketHistoryView;