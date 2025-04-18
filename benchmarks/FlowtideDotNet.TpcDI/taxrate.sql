CREATE VIEW TaxRateView AS
SELECT
  TX_ID,
  TX_NAME,
  TX_RATE
FROM taxrate_raw;

INSERT INTO blackhole
SELECT * FROM TaxRateView;