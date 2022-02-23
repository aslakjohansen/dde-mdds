# DDE - Metadata Discovery Service

There are three components:
1. **Collector** Moves data from yet uncharacterized substreams embedded in a single kafka stream to the **Storage**. This role is implemented by the [ingress](dde-mdds-ingress) component.
2. **Storage** Database which stores timeseries data until enough has been collected for processing and a list of processed timeseries. This role is implemented by the [postgres](dde-mdds-postgres) component.
3. **Analyzer** Periodically scans the **Storage** for timeseries that are ripe for processing, processes them and marks them as processed. This role is implemented by the [process](dde-mdds-process) component.

Additionally, there is a [kafka](deps/kafka) component for testing purposes. How these components are connected can be configured through environment variables. Defaults exists for each environment variable in the [ingress](dde-mdds-postgres) and [process](dde-mdds-process). The first thing these components do is to print out their configuration along with the names of the environment variables relevant of changing it. Dockerfiles exists for all components.

