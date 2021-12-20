# DDE - Metadata Discovery Service

There are three components:
1. **Collector** Moves data from yet uncharacterized substreams embedded in a single kafka stream to the **Storage**.
2. **Storage** Database which stores timeseries data until enough has been collected for processing and a list of processed timeseries.
3. **Analyzer** Periodically scans the **Storage** for timeseries that are ripe for processing, processes them and marks them as processed.

