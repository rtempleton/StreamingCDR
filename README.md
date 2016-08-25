StreamingCDR
===

Adaptation of a batch project I built previously using Pervasive DataRush now utilizing Storm and Kafka. CDR records from a Veraz switch would either be streamed directly to Kafka or assisted by some middle ware such as Apache NiFi. The CDR records are consumed by the Storm topology which performs filtering and data transformation/enrichment on records.

1. Calls are "bucketed" into 15 minute time window 12:00 - 12:14:59 = 1, 12:15 - 12:29:59 = 2, etc - Simplifies aggregations as finer granularity is not required
2. Geo enriches domestic calls by determining lat/lon based on NPA (Area Code) - granularity to NPA/NXX level
3. Geo enriches international calls by deriving country code from Dialed Number.
4. Calculates ingress and egress post dial delay.
5. Early event data present is the record is pivoted in the record. **ALL** routing attempts for a given call are derived including vendor and release code and route order.

WIP
---

1. Will use Storm tumbling window features to calculate ASR and Adjusted ASR in real time. Info to be made available to operational dashboards for real-time overview of network routing performance.
2. Raw records stored in HDFS
3. Enriched data stored in Hive for drill down