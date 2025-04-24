CREATE VIEW CustomerMgmt AS
SELECT * FROM customers_history;

CREATE VIEW customers_all_events AS
SELECT
  CAST(c.Customer.C_ID AS INT) as CustomerID,
  c.Customer.C_TAX_ID AS TaxID,
  CASE 
    WHEN c.ActionType IN ('NEW', 'UPDCUST') THEN 'ACTIVE'
    ELSE 'INACTIVE'
  END as Status,
  c.Customer.Name.C_L_NAME AS LastName,
  c.Customer.Name.C_F_NAME AS FirstName,
  c.Customer.Name.C_M_NAME AS MiddleName,
  c.Customer.C_TIER as Tier,
  c.Customer.C_DOB as DOB,
  c.Customer.ContactInfo.C_PRIM_EMAIL as Email1,
  c.Customer.ContactInfo.C_ALT_EMAIL as Email2,
  UPPER(c.Customer.C_GNDR) AS Gender,
  c.Customer.Address.C_ADLINE1 as AddressLine1,
  c.Customer.Address.C_ADLINE2 as AddressLine2,
  c.Customer.Address.C_ZIPCODE as PostalCode,
  c.Customer.Address.C_CITY as City,
  c.Customer.Address.C_STATE_PROV as StateProv,
  c.Customer.Address.C_CTRY as Country,
  c.Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE AS C_CTRY_1,
  c.Customer.ContactInfo.C_PHONE_1.C_AREA_CODE AS C_AREA_1,
  c.Customer.ContactInfo.C_PHONE_1.C_LOCAL AS C_LOCAL_1,
  c.Customer.ContactInfo.C_PHONE_1.C_EXT AS C_EXT_1,
  c.Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE AS C_CTRY_2,
  c.Customer.ContactInfo.C_PHONE_2.C_AREA_CODE AS C_AREA_2,
  c.Customer.ContactInfo.C_PHONE_2.C_LOCAL AS C_LOCAL_2,
  c.Customer.ContactInfo.C_PHONE_2.C_EXT AS C_EXT_2,
  c.Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE AS C_CTRY_3,
  c.Customer.ContactInfo.C_PHONE_3.C_AREA_CODE AS C_AREA_3,
  c.Customer.ContactInfo.C_PHONE_3.C_LOCAL AS C_LOCAL_3,
  c.Customer.ContactInfo.C_PHONE_3.C_EXT AS C_EXT_3,
  c.Customer.TaxInfo.C_NAT_TX_ID as C_NAT_TX_ID,
  c.Customer.TaxInfo.C_LCL_TX_ID as C_LCL_TX_ID,
  c.ActionTS,
  1 AS BatchID,
  c.BatchDate
FROM CustomerMgmt c
WHERE c.ActionType IN ('NEW', 'UPDCUST', 'INACT');

-- This view contains all customer updates and resolves to their latest values
CREATE VIEW customers_base AS
SELECT
  CustomerID,
  LAST_VALUE(TaxID) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as TaxID,
  Status,
  LAST_VALUE(LastName) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as LastName,
  LAST_VALUE(FirstName) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as FirstName,
  LAST_VALUE(MiddleName) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as MiddleName,
  LAST_VALUE(Tier) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as Tier,
  LAST_VALUE(DOB) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as DOB,
  LAST_VALUE(Email1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as Email1,
  LAST_VALUE(Email2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as Email2,
  LAST_VALUE(Gender) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as Gender,
  LAST_VALUE(AddressLine1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as AddressLine1,
  LAST_VALUE(AddressLine2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as AddressLine2,
  LAST_VALUE(PostalCode) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as PostalCode,
  LAST_VALUE(City) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as City,
  LAST_VALUE(StateProv) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as StateProv,
  LAST_VALUE(Country) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as Country,
  LAST_VALUE(C_CTRY_1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_CTRY_1,
  LAST_VALUE(C_AREA_1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_AREA_1,
  LAST_VALUE(C_LOCAL_1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_LOCAL_1,
  LAST_VALUE(C_EXT_1) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_EXT_1,
  LAST_VALUE(C_CTRY_2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_CTRY_2,
  LAST_VALUE(C_AREA_2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_AREA_2,
  LAST_VALUE(C_LOCAL_2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_LOCAL_2,
  LAST_VALUE(C_EXT_2) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_EXT_2,
  LAST_VALUE(C_CTRY_3) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_CTRY_3,
  LAST_VALUE(C_AREA_3) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_AREA_3,
  LAST_VALUE(C_LOCAL_3) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_LOCAL_3,
  LAST_VALUE(C_EXT_3) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_EXT_3,
  LAST_VALUE(C_NAT_TX_ID) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_NAT_TX_ID,
  LAST_VALUE(C_LCL_TX_ID) IGNORE NULLS OVER (PARTITION BY CustomerID ORDER BY ActionTS) as C_LCL_TX_ID,
  ActionTS,
  floor_timestamp_day(ActionTS) AS EffectiveDate,
  BatchID,
  BatchDate
FROM customers_all_events;

CREATE VIEW customers_incremental AS
SELECT
  C_ID as CustomerID,
  C_TAX_ID as TaxID,
  CASE 
    WHEN C_ST_ID = 'ACTV' THEN 'ACTIVE'
    ELSE 'INACTIVE'
  END as Status,
  C_L_NAME as LastName,
  C_F_NAME as FirstName,
  C_M_NAME as MiddleName,
  C_TIER as Tier,
  C_DOB as DOB,
  C_EMAIL_1 as Email1,
  C_EMAIL_2 as Email2,
  C_GNDR as Gender,
  C_ADLINE1 as AddressLine1,
  C_ADLINE2 as AddressLine2,
  C_ZIPCODE as PostalCode,
  C_CITY as City,
  C_STATE_PROV as StateProv,
  C_CTRY as Country,
  C_CTRY_1,
  C_AREA_1,
  C_LOCAL_1,
  C_EXT_1,
  C_CTRY_2,
  C_AREA_2,
  C_LOCAL_2,
  C_EXT_2,
  C_CTRY_3,
  C_AREA_3,
  C_LOCAL_3,
  C_EXT_3,
  C_NAT_TX_ID,
  C_LCL_TX_ID,
  ActionTS,
  floor_timestamp_day(ActionTS) AS EffectiveDate,
  BatchID,
  ActionTS AS BatchDate
FROM customers_incremental_raw;

CREATE VIEW customers_all AS
SELECT * FROM customers_base
UNION ALL
SELECT * FROM customers_incremental;


CREATE VIEW CustomerHistory AS
SELECT
  surrogate_key_int64() OVER (PARTITION BY CustomerID, EffectiveDate) as SK_CustomerID,
  CustomerID,
  TaxID,
  Status,
  LastName,
  FirstName,
  MiddleName,
  Tier,
  DOB,
  Email1,
  Email2,
  Gender,
  AddressLine1,
  COALESCE(AddressLine2, '') AS AddressLine2,
  PostalCode,
  City,
  StateProv,
  Country,
  CASE 
    WHEN C_CTRY_1 IS NOT NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL THEN
	  CONCAT('+', C_CTRY_1, ' (', C_AREA_1, ') ', C_LOCAL_1, C_EXT_1)
    WHEN C_CTRY_1 IS NULL AND C_AREA_1 IS NOT NULL AND C_LOCAL_1 IS NOT NULL THEN
	  CONCAT('(', C_AREA_1, ') ', C_LOCAL_1, C_EXT_1)
    WHEN C_CTRY_1 IS NULL AND C_AREA_1 IS NULL AND C_LOCAL_1 IS NOT NULL THEN
      CONCAT(C_LOCAL_1, C_EXT_1)
    ELSE NULL
  END AS Phone1,
   CASE 
    WHEN C_CTRY_2 IS NOT NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL THEN
	  CONCAT('+', C_CTRY_2, ' (', C_AREA_2, ') ', C_LOCAL_2, C_EXT_2)
    WHEN C_CTRY_2 IS NULL AND C_AREA_2 IS NOT NULL AND C_LOCAL_2 IS NOT NULL THEN
	  CONCAT('(', C_AREA_2, ') ', C_LOCAL_2, C_EXT_2)
    WHEN C_CTRY_2 IS NULL AND C_AREA_2 IS NULL AND C_LOCAL_2 IS NOT NULL THEN
      CONCAT(C_LOCAL_2, C_EXT_2)
    ELSE NULL
  END AS Phone2,
  CASE 
    WHEN C_CTRY_3 IS NOT NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL THEN
	  CONCAT('+', C_CTRY_3, ' (', C_AREA_3, ') ', C_LOCAL_3, C_EXT_3)
    WHEN C_CTRY_3 IS NULL AND C_AREA_3 IS NOT NULL AND C_LOCAL_3 IS NOT NULL THEN
	  CONCAT('(', C_AREA_3, ') ', C_LOCAL_3, C_EXT_3)
    WHEN C_CTRY_3 IS NULL AND C_AREA_3 IS NULL AND C_LOCAL_3 IS NOT NULL THEN
      CONCAT(C_LOCAL_3, C_EXT_3)
    ELSE NULL
  END AS Phone3,
  EffectiveDate,
  ntr.TX_NAME AS NationalTaxRateDesc,
  ntr.TX_RATE AS NationalTaxRate,
  ltr.TX_NAME AS LocalTaxRateDesc,
  ltr.TX_RATE AS LocalTaxRate,
  COALESCE(LEAD(EffectiveDate) OVER (PARTITION BY CustomerID ORDER BY EffectiveDate), CAST('9999-12-31' as TIMESTAMP)) AS EndDate,
  LEAD(EffectiveDate) OVER (PARTITION BY CustomerID ORDER BY EffectiveDate) IS NULL AS IsCurrent,
  c.BatchID,
  c.BatchDate
FROM customers_all c
LEFT JOIN TaxRateView ntr
ON C_NAT_TX_ID = ntr.TX_ID
LEFT JOIN TaxRateView ltr
ON C_LCL_TX_ID = ltr.TX_ID
WHERE ROW_NUMBER() OVER (PARTITION BY CustomerID, EffectiveDate ORDER BY ActionTS DESC) = 1;
