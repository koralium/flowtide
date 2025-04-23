CREATE VIEW ProspectView AS
SELECT
 AgencyID,
 CAST(timestamp_format(RecordDate, 'yyyyMMdd') as INT) as SK_RecordDateID,
 CAST(timestamp_format(UpdateDate, 'yyyyMMdd') as INT) as SK_UpdateDateID,
 p.BatchID,
 c.CustomerID is not null AS IsCustomer,
 p.LastName,
 p.FirstName,
 MiddleInitial,
 p.Gender,
 p.AddressLine1,
 p.AddressLine2,
 p.PostalCode,
 p.City,
 p.State,
 p.Country,
 Phone,
 Income,
 NumberCars,
 NumberChildren,
 MaritalStatus,
 Age,
 CreditRating,
 OwnOrRentFlag,
 Employer,
 NumberCreditCards,
 NetWorth,
 string_join('+', LIST(
	 CASE WHEN NetWorth > 1000000 OR Income > 200000 THEN 'HighIncome' ELSE NULL END,
	 CASE WHEN NumberChildren > 3 OR NumberCreditCards > 5 THEN 'Expenses' ELSE NULL END,
	 CASE WHEN Age > 45 THEN 'Boomer' ELSE NULL END,
	 CASE WHEN Income < 50000 OR CreditRating < 600 OR NetWorth < 100000 THEN 'MoneyAlert' ELSE NULL END,
	 CASE WHEN NumberCars > 3 OR NumberCreditCards > 7 THEN 'Spender' ELSE NULL END,
	 CASE WHEN Age < 25 AND NetWorth > 1000000 THEN 'Inherited' ELSE NULL END
 )) AS MarketingNameplate
 FROM prospects_raw p
LEFT JOIN CustomerHistory c
ON 
	UPPER(p.FirstName) = UPPER(c.FirstName) AND
	UPPER(p.LastName) = UPPER(c.LastName) AND
	UPPER(p.AddressLine1) = UPPER(c.AddressLine1) AND
	UPPER(p.AddressLine2) = UPPER(c.AddressLine2) AND
	UPPER(p.PostalCode) = UPPER(c.PostalCode) AND
	c.IsCurrent = true AND
	c.Status = 'ACTIVE';

INSERT INTO sink.Prospect
SELECT * FROM ProspectView;