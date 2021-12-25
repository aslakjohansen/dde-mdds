-- timeseries metadata
CREATE TABLE metadata(
  id SERIAL PRIMARY KEY,
  device_id CHAR(16) NOT NULL,
  sensor_id CHAR(99) NOT NULL,
  UNIQUE (device_id, sensor_id)
);

-- raw samples
CREATE TABLE samples(
  id SERIAL PRIMARY KEY,
  metadata_id INT,
  time  TIMESTAMP,
  value REAL,
  CONSTRAINT fk_metadata FOREIGN KEY(metadata_id) REFERENCES metadata(id)
);

-- List of tables which have been processed
CREATE TABLE control(
  id SERIAL PRIMARY KEY,
  metadata_id INT,
  processed BOOLEAN,
  CONSTRAINT fk_metadata FOREIGN KEY(metadata_id) REFERENCES metadata(id)
);

