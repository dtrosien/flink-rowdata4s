package com.dtrosien.rowdata4s

import com.sksamuel.avro4s.AvroSchema
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.{INT, MAP, STRING}
import org.apache.flink.table.types.DataType

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.*
import java.util.UUID
import scala.jdk.CollectionConverters.*

class EncoderTest extends UnitSpec:

  "Encoder" should "convert primitives" in {

    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)

    val schema = AvroSchema[Primitives]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val primitives = Primitives(42, 42L, 42.42, 42.42f, true)

    val toRowData: ToRowData[Primitives] = ToRowData.apply[Primitives](logicalType)

    val rowData = toRowData.to(primitives)

    rowData.getInt(0) shouldBe 42
    rowData.getLong(1) shouldBe 42L
    rowData.getDouble(2) shouldBe 42.42
    rowData.getFloat(3) shouldBe 42.42f
    rowData.getBoolean(4) shouldBe true

  }

  it should "convert optionals" in {

    case class Optionals(maybeInt: Option[Int], maybeString: Option[String])

    val schema = AvroSchema[Optionals]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val optionals = Optionals(Some(42), None)

    val toRowData: ToRowData[Optionals] = ToRowData.apply[Optionals](logicalType)

    val rowData = toRowData.to(optionals)

    rowData.getInt(0) shouldBe 42
    rowData.getString(1) shouldBe null
  }

  it should "convert nested data" in {
    case class Inner(name: String)
    case class Outer(id: Int, inner: Inner)

    val schema = AvroSchema[Outer]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val outer = Outer(42, Inner("someName"))

    val toRowData: ToRowData[Outer] = ToRowData.apply[Outer](logicalType)

    val rowData = toRowData.to(outer)

    rowData.getRow(1, 1).getString(0).toString shouldBe "someName"

  }

  it should "convert String" in {

    case class Strings(uuid: UUID, str: String, charSequence: CharSequence)
    val schema      = AvroSchema[Strings]
    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val uuid                          = UUID.randomUUID()
    val strings                       = Strings(uuid, "string", "chars")
    val toRowData: ToRowData[Strings] = ToRowData.apply[Strings](logicalType)

    val rowData = toRowData.to(strings)

    rowData.getString(0).toString shouldBe uuid.toString
    rowData.getString(1).toString shouldBe "string"
    rowData.getString(2).toString shouldBe "chars"

  }

  it should "convert collections" in {
    case class Inner(name: String)
    case class Collections(
        list: List[Boolean],
        map: Map[String, Inner],
        arr: Array[Option[Inner]],
        seq: Seq[Int],
        vec: Vector[String]
    )

    val inner = Inner("someName")
    val collections = Collections(
      List(true, false),
      Map("key" -> inner),
      Array(Some(inner), None, Some(inner)),
      Seq(1, 2),
      Vector("1", "2")
    )

    val schema                            = AvroSchema[Collections]
    val logicalType                       = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType
    val toRowData: ToRowData[Collections] = ToRowData.apply[Collections](logicalType)

    val rowData = toRowData.to(collections)

    rowData.getArray(0).getBoolean(1) shouldBe false
    rowData.getMap(1).valueArray().getRow(0, 1).getString(0).toString shouldBe "someName"
    rowData.getArray(2).getRow(1, 1) shouldBe null
    rowData.getArray(3).getInt(1) shouldBe 2
    rowData.getArray(4).getString(0).toString shouldBe "1"
  }

  it should "convert custom key types in maps" in {
    val customKeyType: DataType = DataTypes.ROW(
      DataTypes.FIELD("uuid", STRING()),
      DataTypes.FIELD("map", MAP(INT(), STRING()))
    )

    val logicalType = customKeyType.getLogicalType

    case class CustomKey(uuid: UUID, map: Map[Int, String])

    val customKey = CustomKey(UUID.randomUUID(), Map(123 -> "address"))

    val toRowData: ToRowData[CustomKey] = ToRowData.apply[CustomKey](logicalType)

    val rowData = toRowData.to(customKey)

    rowData.getMap(1).keyArray().getInt(0) shouldBe 123
  }

  it should "convert big decimals" in {
    case class Deci(bigDecimal: BigDecimal)
    val schema                     = AvroSchema[Deci]
    val deci                       = Deci(BigDecimal.valueOf(123))
    val logicalType                = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](logicalType)

    val rowData = toRowData.to(deci)

    rowData.getDecimal(0, 8, 2).toBigDecimal.longValue() shouldBe 123L

  }

  it should "convert bytes" in {
    case class Bytes(bytes: Array[Byte], bytebuffer: ByteBuffer)
    val schema = AvroSchema[Bytes]
    val bytes  = Bytes("asas".getBytes, ByteBuffer.wrap("asas".getBytes))

    val logicalType                 = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType
    val toRowData: ToRowData[Bytes] = ToRowData.apply[Bytes](logicalType)

    val rowData = toRowData.to(bytes)

    rowData.getBinary(0) shouldBe "asas".getBytes
    rowData.getBinary(1) shouldBe "asas".getBytes
  }

  it should "convert tuples" in {
    case class Tup(tuple: (String, String))
    val schema                    = AvroSchema[Tup]
    val logicalType               = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType
    val toRowData: ToRowData[Tup] = ToRowData.apply[Tup](logicalType)
    val tup                       = Tup(("3", "3"))
    val rowData                   = toRowData.to(tup)

    rowData.getRow(0, 2).getString(0).toString shouldBe "3"
  }

  it should "convert temporal types" in {
    case class TimeAndDates(
        localDateTime: LocalDateTime,
        date: Date,
        instant: Instant,
        localDate: LocalDate,
        timestamp: Timestamp,
        localTime: LocalTime,
        offsetDateTime: OffsetDateTime
    )

    val testInstant = Instant.now
    val timeAndDates = TimeAndDates(
      localDateTime = LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC),
      date = Date.valueOf(LocalDate.ofInstant(testInstant, ZoneOffset.UTC)),
      instant = testInstant, // gets converted to timestamp
      localDate = LocalDate.ofInstant(testInstant, ZoneOffset.UTC),
      timestamp = Timestamp.from(testInstant), // as flink timestamp
      localTime = LocalTime.ofInstant(testInstant, ZoneOffset.UTC),
      offsetDateTime = OffsetDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    )

    val schema      = AvroSchema[TimeAndDates]
    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val toRowData: ToRowData[TimeAndDates] = ToRowData.apply[TimeAndDates](logicalType)
    val rowData                            = toRowData.to(timeAndDates)


    // checks
    rowData.getLong(0) shouldBe testInstant.toEpochMilli
    rowData.getInt(1) shouldBe LocalDate.ofInstant(testInstant, ZoneOffset.UTC).toEpochDay
    rowData.getTimestamp(2, 8).toInstant shouldBe testInstant
    rowData.getInt(3) shouldBe LocalDate.ofInstant(testInstant, ZoneOffset.UTC).toEpochDay

    // flink timestamp is transformed to LocalDateTimeFirst before getting converted to Timestamp
    rowData.getTimestamp(4, 8).toTimestamp shouldBe Timestamp.valueOf(
      LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    )

    rowData.getLong(5) shouldBe LocalTime.ofInstant(testInstant, ZoneOffset.UTC).toNanoOfDay

    OffsetDateTime
      .parse(rowData.getString(6).toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
      .toInstant shouldBe OffsetDateTime
      .ofInstant(testInstant, ZoneOffset.UTC)
      .toInstant // use of instant to have stable tests

  }
