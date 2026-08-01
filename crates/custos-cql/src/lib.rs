//! CQL (Cassandra) wire-protocol adapter over the common CustosDB core.
//!
//! Placeholder skeleton. The CQL surface maps onto the same Dynamo-lineage core
//! as the DynamoDB adapter (ADR 0006): a CQL partition key + clustering columns
//! correspond to a partition key + sort key over the `StorageEngine`, exactly
//! the mapping that `custos-dynamo`'s item API already demonstrates. What
//! remains CQL-specific is the wire protocol framing and a CQL parser/type
//! system; that is future work.
