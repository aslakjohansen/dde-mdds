-- timeseries metadata
CREATE TABLE metadata(
  id INT PRIMARY KEY NOT NULL,
  device_id CHAR(16) NOT NULL,
  sensor_id CHAR(99) NOT NULL,
);

-- raw samples
CREATE TABLE samples(
  id INT PRIMARY KEY NOT NULL,
  metadata_id INT,
  time  TIMESTAMP,
  value REAL,
  CONSTRAINT fk_metadata FOREIGH KEY(metadata_id) REFERENCES metadata(id),
);

-- List of tables which have been processed
CREATE TABLE control(
  id INT PRIMARY KEY NOT NULL,
  metadata_id INT,
  CONSTRAINT fk_metadata FOREIGH KEY(metadata_id) REFERENCES metadata(id),
);

