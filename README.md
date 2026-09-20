# flink-rowdata4s

Encode Scala case classes to Apache Flink `RowData` and back, driven by a Flink schema.

`RowData` is Flink's internal row format. Table connectors such as the Iceberg sink consume it and the Iceberg source
produces it, but it has no generic accessors: reading a column means calling `getInt`, `getString`, `getTimestamp`
with the right position and type. This library derives those calls from a case class and the table's `RowType`.

- Scala 3, Flink 1.20 (`flink-table-api-java` is a `provided` dependency).
- Derivation via Magnolia; sealed traits, enums, nested case classes, collections, maps, options and the usual
  temporal types are supported out of the box.

```scala
libraryDependencies += "io.github.dtrosien" %% "flink-rowdata4s" % "<version>"
```

## Usage

```scala
import com.dtrosien.rowdata4s.{FromRowData, ToRowData}
import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.apache.flink.table.types.logical.RowType

case class Event(id: Long, name: String, at: Instant, amount: Option[BigDecimal])

// the schema the codec works against, here derived from the case class
val rowType: RowType = FlinkDataType[Event].getLogicalType.asInstanceOf[RowType]

val toRowData   = ToRowData[Event](rowType)
val fromRowData = FromRowData[Event](rowType)

val row: RowData = toRowData.to(Event(1, "a", Instant.now, Some(BigDecimal("9.99"))))
val event: Event = fromRowData.from(row)
```

Both codecs are Java serializable and can be held in Flink functions.

### With Iceberg

Take the `RowType` from the table so the codec matches the columns the table actually has, and attach the same type
to the stream so that shuffles serialize rows in the layout the Iceberg writer expects:

```scala
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.iceberg.flink.{FlinkSchemaUtil, TableLoader}
import org.apache.iceberg.flink.sink.FlinkSink

val tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of("db", "events"))
tableLoader.open()
val rowType = FlinkSchemaUtil.convert(tableLoader.loadTable().schema())

val toRowData = ToRowData[Event](rowType)

val rows: DataStream[RowData] = events
  .map(event => toRowData.to(event))
  .returns(InternalTypeInfo.of(rowType))

FlinkSink.forRowData(rows).tableLoader(tableLoader).append()
```

Reading is the mirror image: `FromRowData[Event](rowType)` on the `RowData` stream of a `FlinkSource`.

To create the table from the case class instead, convert the derived schema:
`FlinkSchemaUtil.convert(FlinkDataType[Event].getLogicalType.asInstanceOf[RowType])` gives the Iceberg schema. Instants
become `timestamptz`, decimals keep precision and scale, `@TableComment` becomes the column doc.

Changelog rows: `toRowData.to(event, RowKind.DELETE)` (or `sourceRow.getRowKind` to forward the kind of an input row).
Iceberg's upsert mode treats `INSERT` as upsert, so `DELETE` is what you need for removals.

## How columns are mapped

- **By name.** Columns are matched to case class parameters by name (`@TableName` is honoured), so the column order of
  the table does not have to match the parameter order. Sealed trait variants are matched by name as well.
- **Decoding a wider table** ignores columns without a parameter. A parameter without a column takes its default
  value, `None` if it is an `Option`, and otherwise `FromRowData` fails when it is created.
- **Encoding** requires a parameter for every column; parameters without a column are not written.
- **NULL** decodes to `None` for `Option` parameters and to the default value for others; without a default the
  record fails, with the field name in the error. A column that cannot be decoded also falls back to the default.
- **Conversions follow the column.** Integer and floating point fields are converted to the column's type (widening
  is lossless, narrowing wraps around like a JVM cast). `BigDecimal` is written with the column's precision and
  scale, extra fractional digits are rounded HALF_UP, only an integer part that does not fit is rejected. Timestamps
  are truncated to the column precision. Strings are truncated to `VARCHAR(n)` and truncated or space-padded to
  `CHAR(n)`; `STRING` is unbounded.
- **Errors** are `RowDataDecodingException` (field name, column type, cause) on decode. Schema mismatches that can
  be detected up front, such as a `Long` field on a `STRING` column, fail when the codec is created.

## Schema derivation

`FlinkDataType[T]` derives a Flink `DataType`:

| Scala type                                    | Flink type                                                      |
|-----------------------------------------------|-----------------------------------------------------------------|
| `Int`, `Byte`, `Short`                        | `INT`                                                           |
| `Long`, `Float`, `Double`, `Boolean`          | `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`                          |
| `String`, `CharSequence`, `UUID`              | `STRING` (see `@TableVarchar`, `@TableChar`)                    |
| `BigDecimal`                                  | `DECIMAL(p, s)` from a `ScalePrecision` given, default `(8, 2)` |
| `Array[Byte]`, `Seq[Byte]`, `ByteBuffer`      | `BYTES`                                                         |
| `Instant`, `java.util.Date`, `OffsetDateTime` | `TIMESTAMP_LTZ(p)`                                              |
| `LocalDateTime`, `java.sql.Timestamp`         | `TIMESTAMP(p)`                                                  |
| `LocalDate`, `java.sql.Date`                  | `DATE`                                                          |
| `LocalTime`                                   | `TIME(3)` (Flink stores milliseconds)                           |
| `Option[T]`                                   | nullable `T`                                                    |
| `Seq`, `List`, `Vector`, `Set`, `Array`       | `ARRAY`                                                         |
| `Map[String, V]`                              | `MAP<STRING, V>`                                                |
| case class, tuple                             | `ROW`                                                           |
| enum, sealed trait of case objects            | `STRING` (the case name)                                        |
| sealed trait, enum with parameterized cases   | `ROW` with one nullable field per variant                       |

`p` comes from a `TimestampPrecision` given and defaults to 6, Flink's default and what Iceberg stores. Any mapping
can be replaced by a `given DataTypeFor[T]` in scope.

`OffsetDateTime` is stored as an instant; the offset is not kept. `MULTISET<T>` columns decode to `Map[T, Int]`.

### Annotations

`com.dtrosien.rowdata4s.annotations`. The codecs follow the schema, so an annotation that changes the column type
changes encoding and decoding as well.

| Annotation                        | Effect                                                               |
|-----------------------------------|----------------------------------------------------------------------|
| `@TableName("col")`               | column name; on a variant, the name of its union field               |
| `@TableTransient()`               | field is left out of the schema                                      |
| `@TableDecimal(precision, scale)` | `DECIMAL(precision, scale)` for this `BigDecimal` field              |
| `@TableTimestampPrecision(p)`     | `TIMESTAMP(p)` / `TIMESTAMP_LTZ(p)` for this temporal field          |
| `@TableVarchar(n)`                | `VARCHAR(n)`; longer values are truncated on encode                  |
| `@TableChar(n)`                   | `CHAR(n)`; values are truncated or space-padded on encode            |
| `@TableComment("text")`           | column description, the column doc of an Iceberg table               |

```scala
case class Payment(
    @TableComment("payment id") @TableVarchar(36) id: String,
    @TableDecimal(18, 4) amount: BigDecimal,
    @TableTimestampPrecision(3) at: Instant,
    @TableName("cc") country: Option[String],
    @TableTransient() cachedTotal: BigDecimal
)
```

## Limitations

- Value classes (`AnyVal`) cannot be derived; provide explicit codecs, e.g. `Encoder[Long].contramap(_.value)`.
- A sealed trait with a single subtype is encoded as that subtype, not as a union `ROW`.
- Union variants must be distinguishable by name; two variants with the same `@TableName` are not supported.

## Credits

The derivation is adapted from [avro4s](https://github.com/sksamuel/avro4s), reworked for Flink's `RowData`.
