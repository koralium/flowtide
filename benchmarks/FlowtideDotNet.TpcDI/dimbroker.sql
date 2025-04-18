CREATE VIEW mindate AS
select 
	min(DateValue) as DateValue
FROM DimDateView;

CREATE VIEW DimBrokerView AS
SELECT 
	e.EmployeeID AS SK_BrokerID,
	e.EmployeeID as BrokerID,
	e.ManagerID,
	e.EmployeeFirstName as FirstName,
	e.EmployeeLastName as LastName, 
	e.EmployeeMI as MiddleInitial,
	e.EmployeeBranch as Branch,
	e.EmployeeOffice as Office,
	e.EmployeePhone as Phone,
	true AS IsCurrent,
	1 AS BatchID,
	md.DateValue as EffectiveDate,
	CAST('9999-12-31' AS TIMESTAMP) as EndDate
FROM hr_raw e
JOIN mindate md ON 1 = 1
WHERE e.EmployeeJobCode = 314; 

INSERT INTO blackhole
SELECT BrokerID FROM DimBrokerView;