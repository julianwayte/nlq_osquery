Provides natural language querying of osquery data. The osquery data is pulled from Kafka and converted to Apache ORC files then put into Hive and queryied via Trino (https://trino.io/). 
The query is built via a call to an LLM. The shortlist of tables to feed into the LLM prompt is first generated via a vector database semantic search.

