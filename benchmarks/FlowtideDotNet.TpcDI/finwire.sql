CREATE VIEW finwire_data AS
SELECT
  substring(value, 1, 15) as PostingDate,
  substring(value, 16, 3) as RecType,
  value
FROM finwire_raw;


