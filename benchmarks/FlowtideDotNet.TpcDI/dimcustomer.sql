CREATE VIEW DimCustomerView AS
SELECT
  c.SK_CustomerID,
  c.CustomerID,
  c.TaxID,
  c.Status,
  c.LastName,
  c.FirstName,
  c.MiddleName,
  c.Tier,
  c.DOB,
  c.Email1,
  c.Email2,
  c.Gender,
  c.AddressLine1,
  c.AddressLine2,
  c.PostalCode,
  c.City,
  c.StateProv,
  c.Country,
  c.Phone1,
  c.Phone2,
  c.Phone3,
  c.NationalTaxRateDesc,
  c.NationalTaxRate,
  c.LocalTaxRateDesc,
  c.LocalTaxRate,
  c.EffectiveDate,
  c.EndDate,
  c.IsCurrent,
  p.AgencyID,
  p.CreditRating,
  p.NetWorth,
  p.MarketingNameplate,
  c.BatchID
FROM CustomerHistory c
LEFT JOIN ProspectView p
ON 
	UPPER(p.FirstName) = UPPER(c.FirstName) AND
	UPPER(p.LastName) = UPPER(c.LastName) AND
	UPPER(p.AddressLine1) = UPPER(c.AddressLine1) AND
	UPPER(p.AddressLine2) = UPPER(c.AddressLine2) AND
	UPPER(p.PostalCode) = UPPER(c.PostalCode) AND
	c.IsCurrent = true;

INSERT INTO blackhole
SELECT * FROM DimCustomerView;