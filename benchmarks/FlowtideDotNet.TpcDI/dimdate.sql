-- Dateview contains the query for DimDate
CREATE VIEW DimDateView AS
SELECT 
	SK_DateID,
	CAST(datevalue as TIMESTAMP) as DateValue,
	DateDesc,
	CalendarYearID,
	CalendarYearDesc,
	CalendarQtrID,
	CalendarQtrDesc,
	CalendarMonthID,
	CalendarMonthDesc,
	CalendarWeekID,
	CalendarWeekDesc,
	DayOfWeekNum,
	DayOfWeekDesc,
	FiscalYearID,
	FiscalYearDesc,
	FiscalQtrID,
	FiscalQtrDesc,
	HolidayFlag
FROM dates_raw;

INSERT INTO sink.DimDate
SELECT * FROM DimDateView;