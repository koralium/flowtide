CREATE VIEW TradeTypeView AS
SELECT
  TT_ID,
  TT_NAME,
  TT_IS_SELL,
  TT_IS_MRKT
FROM tradetype_raw;

INSERT INTO sink.TradeType
SELECT * FROM TradeTypeView;