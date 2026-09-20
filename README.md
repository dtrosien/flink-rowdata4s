# flink-rowdata4s

This library provides functionality to encode and decode [Apache Flink](https://flink.apache.org/) `RowData` to and from
Scala `Product` types (e.g., case classes). It is intended to simplify interoperability between Flink's sql data
representations and Scala data models.

A significant portion of this library's implementation is adapted from the [avro4s](https://github.com/sksamuel/avro4s)
project and modified to work with Flink's `RowData` format instead of Avro.

Credit goes to the original authors of avro4s for the foundation on which this library is built.

#### Schema derivation

`FlinkDataType[T]` derives a Flink `DataType` for a case class. Every field maps to the Flink type it is the
conversion class of:

| Scala type                                        | Flink type                              |
|---------------------------------------------------|-----------------------------------------|
| `Int`, `Byte`, `Short`                            | `INT`                                   |
| `Long`, `Float`, `Double`, `Boolean`              | `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`  |
| `String`, `CharSequence`, `UUID`                  | `STRING`; see `@TableVarchar` / `@TableChar` |
| `BigDecimal`                                      | `DECIMAL(p, s)` from a `ScalePrecision` given, default `(8, 2)` |
| `Array[Byte]`, `Seq[Byte]`, `ByteBuffer`          | `BYTES`                                 |
| `Instant`, `java.util.Date`, `OffsetDateTime`     | `TIMESTAMP_LTZ(p)`                      |
| `LocalDateTime`, `java.sql.Timestamp`             | `TIMESTAMP(p)`                          |
| `LocalDate`, `java.sql.Date`                      | `DATE`                                  |
| `LocalTime`                                       | `TIME(3)` (Flink stores milliseconds)   |
| `Option[T]`                                       | nullable `T`                            |
| `Seq`, `List`, `Vector`, `Set`, `Array`           | `ARRAY`                                 |
| `Map[String, V]`                                  | `MAP<STRING, V>`                        |
| case class, tuple                                 | `ROW`                                   |
| enum, sealed trait of case objects                | `STRING`                                |
| sealed trait                                     | `ROW` with one nullable field per variant |

`p` comes from a `TimestampPrecision` given and defaults to 6, Flink's default and what Iceberg stores; use
`TimestampPrecision(3)` for Flink's compact millisecond representation. Any mapping can be replaced by putting your
own `given DataTypeFor[T]` in scope.

Field annotations (package `com.dtrosien.rowdata4s.annotations`) adjust single columns; the codecs follow the
schema, so they apply to encoding and decoding as well:

| Annotation                        | Effect                                                              |
|-----------------------------------|---------------------------------------------------------------------|
| `@TableName("col")`               | column name (also for the union field of a sealed trait / enum case) |
| `@TableTransient()`               | field is left out of the schema                                     |
| `@TableDecimal(precision, scale)` | `DECIMAL(precision, scale)` for this `BigDecimal` field             |
| `@TableTimestampPrecision(p)`     | `TIMESTAMP(p)` / `TIMESTAMP_LTZ(p)` for this temporal field         |
| `@TableComment("text")`           | column description (becomes the column doc of an Iceberg table)     |
| `@TableVarchar(n)`                | `VARCHAR(n)` for this `String` field; longer values are truncated on encode |
| `@TableChar(n)`                   | `CHAR(n)` for this `String` field; values are truncated or space-padded on encode |

```scala
case class Payment(
    @TableComment("payment id") id: String,
    @TableDecimal(18, 4) amount: BigDecimal,
    @TableTimestampPrecision(3) at: Instant,
    @TableTransient() cachedTotal: BigDecimal
)
```

Algebraic data types (ADTs) are flattened into a `Row` containing optional fields for each variant; only the field of
the actual variant is set. Simple enums are treated as Strings.

#### TODO

- [x] Implement Sealed Trait and Enum Encoder
- [x] Implement Sealed Trait and Enum Decoder
- [x] Fix Sealed Trait in DataType derivation
- [ ] add examples to readme
