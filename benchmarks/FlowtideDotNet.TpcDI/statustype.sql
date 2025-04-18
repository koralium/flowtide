CREATE VIEW StatusTypeView AS
SELECT
  ST_ID,
  ST_NAME
FROM statustype_raw;

INSERT INTO blackhole
SELECT * FROM StatusTypeView;