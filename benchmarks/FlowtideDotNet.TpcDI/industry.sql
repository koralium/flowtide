CREATE VIEW IndustryView AS
SELECT
  IN_ID,
  IN_NAME,
  IN_SC_ID
FROM industry_raw;

INSERT INTO blackhole
SELECT * FROM IndustryView;