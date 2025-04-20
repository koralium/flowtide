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
  --ds.SK_SecurityID,
  --ds.SK_CompanyID,
  DM_CLOSE as ClosePrice,
  DM_HIGH as DayHigh,
  DM_LOW as DayLow,
  DM_VOL as Volume,
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
--INNER JOIN DimSecurityView ds
--ON ds.Symbol = dm.DM_S_SYMB AND
--dm.DM_DATE >= ds.EffectiveDate AND
--dm.DM_DATE <= ds.EndDate;

INSERT INTO blackhole
SELECT * FROM earnings_per_year_company;

INSERT INTO console
SELECT * FROM markethistory_base;