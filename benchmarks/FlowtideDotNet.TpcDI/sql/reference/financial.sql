
CREATE VIEW financial_flattened AS
SELECT
  PostingDate as PTS,
  CAST(TRIM(substring(value, 19, 4)) AS INT) as Year,
  CAST(TRIM(substring(value, 23, 1)) AS INT) as Quarter,
  timestamp_parse(TRIM(substring(value, 24, 8)), 'yyyyMMdd') as QtrStartDate,
  TRIM(substring(value, 32, 8)) as PostingDate,
  CAST(TRIM(substring(value, 40, 17)) AS decimal) as Revenue,
  CAST(TRIM(substring(value, 57, 17)) AS decimal) as Earnings,
  CAST(TRIM(substring(value, 74, 12)) as decimal) as EPS,
  CAST(TRIM(substring(value, 86, 12)) as decimal) as DilutedEPS,
  CAST(TRIM(substring(value, 98, 12)) as decimal) as Margin,
  CAST(TRIM(substring(value, 110, 17)) as decimal) as Inventory,
  CAST(TRIM(substring(value, 127, 17)) as decimal) as Assets,
  CAST(TRIM(substring(value, 144, 17)) as decimal) as Liabilities,
  CAST(TRIM(substring(value, 161, 13)) as decimal) as ShOut,
  CAST(TRIM(substring(value, 174, 13)) as decimal) as DilutedShOut,
  TRIM(substring(value, 187, 60)) as CoNameOrCIK
FROM finwire_data
WHERE RecType = 'FIN';

CREATE VIEW FinancialView AS
SELECT
    c.SK_CompanyID AS SK_CompanyID,
	Year AS FI_YEAR,
	Quarter AS FI_QTR,
	QtrStartDate AS FI_QTR_START_DATE,
	Revenue AS FI_REVENUE,
	Earnings AS FI_NET_EARN,
	EPS AS FI_BASIC_EPS,
	DilutedEPS AS FI_DILUT_EPS,
	Margin AS FI_MARGIN,
	Inventory AS FI_INVENTORY,
	Assets AS FI_ASSETS,
	Liabilities AS FI_LIABILITY,
	ShOut AS FI_OUT_BASIC,
	DilutedShOut AS FI_OUT_DILUT
FROM financial_flattened f
INNER JOIN DimCompanyCoNameOrCIK c
ON c.CoNameOrCIK = f.CoNameOrCIK
AND c.EffectiveDate <= f.PTS
AND c.EndDate > f.PTS;


INSERT INTO sink.Financial
SELECT * FROM FinancialView;