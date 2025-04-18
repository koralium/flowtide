CREATE VIEW company_base AS
SELECT
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
TRIM(substring(value, 394, 150)) AS description
FROM finwire_data
WHERE RecType = 'CMP';

CREATE VIEW DimCompanyView as
SELECT
  CIK AS CompanyID,
  CompanyName AS Name,
  spRating as SPRating,
  ceoName AS CEO,
  Description,
  FoundingDate,
  i.IN_NAME AS Industry,
  st.ST_NAME as Status
FROM company_base c
INNER JOIN StatusTypeView st
ON c.Status = st.ST_ID
INNER JOIN IndustryView i
ON c.IndustryId = i.IN_ID;

INSERT INTO console
SELECT * FROM DimCompanyView;
--CREATE VIEW DimCompanyView AS
--SELECT
