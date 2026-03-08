# flink-rowdata4s

This library provides functionality to encode and decode [Apache Flink](https://flink.apache.org/) `RowData` to and from
Scala `Product` types (e.g., case classes). It is intended to simplify interoperability between Flink's sql data
representations and Scala data models.

A significant portion of this library's implementation is adapted from the [avro4s](https://github.com/sksamuel/avro4s)
project and modified to work with Flink's `RowData` format instead of Avro.

Credit goes to the original authors of avro4s for the foundation on which this library is built.

Datatype derivation is compatible with Flink’s `AvroSchemaConverter` only for product types (i.e., case classes /
records).
Algebraic data types (ADTs) are flattened into a `Row` containing optional fields for each variant. This structural
encoding is not compatible with `AvroSchemaConverter`, which does not derive a corresponding Avro union schema and
instead falls back to Kryo serialization for arbitrary objects. Simple enums are treated as Strings inside a Row.

#### TODO

- [x] Implement Sealed Trait and Enum Encoder
- [x] Implement Sealed Trait and Enum Decoder
- [x] Fix Sealed Trait in DataType derivation
- [ ] add examples to readme
