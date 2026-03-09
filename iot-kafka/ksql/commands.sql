CREATE STREAM SENSORS_RAW (
  event_id VARCHAR,
  device_id VARCHAR,
  timestamp VARCHAR,
  temperature DOUBLE,
  humidity DOUBLE,
  battery DOUBLE
) WITH (
  KAFKA_TOPIC='iot.sensors',
  VALUE_FORMAT='JSON'
);

CREATE STREAM HIGH_TEMP AS
  SELECT device_id, event_id, timestamp, temperature
  FROM SENSORS_RAW
  WHERE temperature >= 27.5
  EMIT CHANGES;

CREATE TABLE TEMP_AVG_PER_MIN AS
  SELECT
    device_id,
    TUMBLINGWINDOW(SIZE 1 MINUTE) AS win,
    AVG(temperature) AS avg_temp
  FROM SENSORS_RAW
  GROUP BY device_id, TUMBLINGWINDOW(SIZE 1 MINUTE)
  EMIT CHANGES;

SHOW STREAMS;
SHOW TABLES;
