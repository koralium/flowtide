CREATE VIEW finwire_data AS
SELECT
  timestamp_parse(substring(value, 1, 15), 'yyyyMMdd-HHmmss') as PostingDate,
  substring(value, 16, 3) as RecType,
  value,
  1 as BatchID
FROM finwire_raw;


