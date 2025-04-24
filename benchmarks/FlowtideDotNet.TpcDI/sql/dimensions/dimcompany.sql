CREATE VIEW company_base AS
SELECT
floor_timestamp_day(PostingDate) as EffectiveDate,
TRIM(substring(value, 19, 60)) as CompanyName,
TRIM(substring(value, 79, 10)) as CIK,
substring(value, 89, 4) as Status,
substring(value, 93, 2) AS IndustryId,
TRIM(substring(value, 95, 4)) AS spRating,
substring(value, 99, 8) AS foundingDate,
TRIM(substring(value, 107, 80)) as addrLine1,
TRIM(substring(value, 187, 80)) as addrLine2,
TRIM(substring(value, 267, 12)) AS postalCode,
TRIM(substring(value, 279, 25)) AS city,
TRIM(substring(value, 304, 20)) AS stateProvice,
TRIM(substring(value, 324, 24)) AS country,
TRIM(substring(value, 348, 46)) AS ceoName,
TRIM(substring(value, 394, 150)) AS description,
batchId
FROM finwire_data
WHERE RecType = 'CMP';

CREATE VIEW DimCompanyView as
SELECT
  surrogate_key_int64() OVER (PARTITION BY CIK, EffectiveDate) as SK_CompanyID,
  CIK AS CompanyID,
  st.ST_NAME as Status,
  CompanyName AS Name,
  i.IN_NAME AS Industry,
  CHECK_VALUE(
    spRating,
	spRating IN ('AAA', 'AA', 'AA+', 'AA-', 'A', 'A+', 'A-', 'BBB', 'BBB+', 'BBB-', 'BB', 'BB+', 'BB-', 'B', 'B+', 'B-', 'CCC', 'CCC+', 'CCC-', 'CC', 'C', 'D'),
	'CO_ID = {CIK}, CO_SP_RATE = {spRating}',
	CIK,
	spRating,
	messageText => 'Invalid SPRating',
	MessageSource => 'DimCompany'
  ) as SPRating,
  CASE 
	WHEN starts_with(spRating, 'A') OR starts_with(spRating, 'BBB') THEN false
	ELSE true
  END AS isLowGrade,
  ceoName AS CEO,
  addrLine1 AS AddressLine1,
  addrLine2 AS AddressLine2,
  postalCode AS PostalCode,
  city AS City,
  stateProvice AS StateProv,
  country AS Country,
  Description,
  timestamp_parse(FoundingDate, 'yyyyMMdd') as FoundingDate,
  LEAD(EffectiveDate) OVER (PARTITION BY CIK ORDER BY EffectiveDate) IS NULL AS IsCurrent,
  BatchID,
  EffectiveDate,
  COALESCE(LEAD(EffectiveDate) OVER (PARTITION BY CIK ORDER BY EffectiveDate), CAST('9999-12-31' as TIMESTAMP)) AS EndDate
FROM company_base c
INNER JOIN StatusTypeView st
ON c.Status = st.ST_ID
INNER JOIN IndustryView i
ON c.IndustryId = i.IN_ID;


-- view to join on either id or name
CREATE VIEW DimCompanyCoNameOrCIK AS
SELECT
  SK_CompanyID,
  companyId as CoNameOrCIK,
  EffectiveDate,
  EndDate
FROM DimCompanyView
UNION
SELECT
  SK_CompanyID,
  Name as CoNameOrCIK,
  EffectiveDate,
  EndDate
FROM DimCompanyView;

INSERT INTO sink.DimCompany
SELECT * FROM DimCompanyView;
