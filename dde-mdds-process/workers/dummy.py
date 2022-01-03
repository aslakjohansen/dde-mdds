#!/usr/bin/env python3

from sys import exit, stderr
import fileinput
import json

readings = []

# read input
for line in fileinput.input():
  line = line.strip()
  if line=="": break
  try:
    es = list(map(float, line.split(" ")))
    if len(es) != 2:
      print("Cannot parse line:", line, file=stderr)
      exit(1)
    readings.append(es)
  except Exception as e:
    print("Exception while attempting to parse line:", e, file=stderr)
    exit(2)

# write output
result = {
  "Unit": [
    {
      "result": "DegC",
      "score": 0.13
    },
    {
      "result": "W",
      "score": 0.08
    },
    {
      "result": "DegF",
      "score": 0.02
    },
  ],
  "Factor": [
    {
      "result": "1",
      "score": 0.17
    },
    {
      "result": "1000",
      "score": 0.074
    },
  ],
  "SensorType": [
    {
      "result": "temperature",
      "score": 0.12
    },
    {
      "result": "energy",
      "score": 0.1
    },
  ],
  "TsDataType": [
    {
      "result": "tællerstand",
      "score": 0.17
    },
    {
      "result": "tilstand",
      "score": 0.089
    },
    {
      "result": "forbrugsmåler",
      "score": 0.006
    },
    {
      "result": "lager",
      "score": 0.0008
    },
  ],
}
print(json.dumps(result, indent = 2, separators=(',', ': ')))
