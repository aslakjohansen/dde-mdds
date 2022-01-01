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
    es = list(map(int, line.split(" ")))
    if len(es) != 2:
      print("Cannot parse line:", line, file=stderr)
      exit(1)
    readings.append(es)
  except Exception as e:
    print("Exception while attempting to parse line:", e, file=stderr)
    exit(2)

# write output
result = {
  "modality": [
    {
      "result": "temperature",
      "score": 0.1
    },
  ],
}
print(json.dumps(result, indent = 2, separators=(',', ': ')))
