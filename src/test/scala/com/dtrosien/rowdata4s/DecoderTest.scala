package com.dtrosien.rowdata4s

import com.sksamuel.avro4s.{AvroName, AvroSchema}
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.{BIGINT, INT, MAP, STRING}
import org.apache.flink.table.data.*
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.Instant.now
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.*
import java.util.UUID
import scala.jdk.CollectionConverters.*


class DecoderTest extends UnitSpec:

  "Decoder" should "convert Primitives" in {
    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)

    val schema = AvroSchema[Primitives]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 5)
      row.setField(0, Int.box(42)) // boxed Int, as Flink fields are Objects
      row.setField(1, Long.box(420L))
      row.setField(2, Double.box(42.42d))
      row.setField(3, Float.box(42.42f))
      row.setField(4, Boolean.box(true))
      row
    }

    val fromRowData: FromRowData[Primitives] = FromRowData.apply[Primitives](logicalType)

    val primitives = fromRowData.from(rowData)

    primitives.int shouldBe 42
    primitives.long shouldBe 420L
    primitives.double shouldBe 42.42d
    primitives.float shouldBe 42.42f
    primitives.boolean shouldBe true

  }

  it should "convert Optionals" in {
    case class Optionals(maybeInt: Option[Int], maybeString: Option[String])

    val schema = AvroSchema[Optionals]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(42)) // boxed Int, as Flink fields are Objects
      row.setField(1, null)
      row
    }

    val fromRowData: FromRowData[Optionals] = FromRowData.apply[Optionals](logicalType)

    val optionals = fromRowData.from(rowData)

    optionals.maybeInt shouldBe Some(42)
    optionals.maybeString shouldBe None

  }

  it should "convert nested data" in {
    case class Test(id: Int, num: Long)
    case class TestNested(id: Int, num: Long, inner: Test)

    val schema = AvroSchema[TestNested]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val testRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(123))
      row.setField(1, Long.box(456L))
      row
    }

    val testNestedRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 3)
      row.setField(0, Int.box(1))
      row.setField(1, Long.box(999L))
      row.setField(2, testRow) // nested RowData
      row
    }

    val fromRowData: FromRowData[TestNested] = FromRowData.apply[TestNested](logicalType)

    val test = fromRowData.from(testNestedRow)

    test.inner.id shouldBe 123

  }

  it should "convert Strings" in {
    case class Strings(uuid: UUID, str: String, charSequence: CharSequence)

    val schema = AvroSchema[Strings]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val uuid = UUID.randomUUID()

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 3)
      row.setField(0, StringData.fromString(uuid.toString)) // boxed Int, as Flink fields are Objects
      row.setField(1, StringData.fromString("testString"))
      row.setField(2, StringData.fromString("testChars"))
      row
    }

    val fromRowData: FromRowData[Strings] = FromRowData.apply[Strings](logicalType)

    val strings = fromRowData.from(rowData)

    strings.uuid shouldBe uuid
    strings.str shouldBe "testString"
    strings.charSequence shouldBe "testChars"
  }

  it should "convert Collections" in {
    case class Inner(name: String)
    case class Collections(
        list: List[Boolean],
        map: Map[String, Inner],
        arr: Array[Option[Inner]],
        seq: Seq[Int],
        vec: Vector[String]
    )

    val schema = AvroSchema[Collections]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val innerRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, StringData.fromString("testString"))
      row
    }
    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 5)
      row.setField(0, GenericArrayData(Array(true, false, true)))
      row.setField(
        1,
        GenericMapData(Map(StringData.fromString("1") -> innerRow, StringData.fromString("2") -> innerRow).asJava)
      )
      row.setField(2, GenericArrayData(Array(null, innerRow.asInstanceOf[Object], null)))
      row.setField(3, GenericArrayData(Array(1, 2, 3)))
      row.setField(
        4,
        GenericArrayData(
          Array(StringData.fromString("1"), StringData.fromString("2"), StringData.fromString("3"))
            .map(_.asInstanceOf[Object])
        )
      )
      row
    }

    val fromRowData: FromRowData[Collections] = FromRowData.apply[Collections](logicalType)

    val collections = fromRowData.from(rowData)

    collections.list shouldBe List(true, false, true)
    collections.map shouldBe Map("1" -> Inner("testString"), "2" -> Inner("testString"))
    collections.arr shouldBe Array(None, Some(Inner("testString")), None)
    collections.seq shouldBe Seq(1, 2, 3)
    collections.vec shouldBe Vector("1", "2", "3")

  }

  it should "convert big decimal" in {
    case class Decimal(dec: BigDecimal)

    val schema = AvroSchema[Decimal]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, DecimalData.fromUnscaledLong(100L, 10, 0))
      row
    }

    val fromRowData: FromRowData[Decimal] = FromRowData.apply[Decimal](logicalType)

    val decimal = fromRowData.from(rowData)

    decimal.dec.toInt shouldBe 100

  }

  it should "convert bytes" in {
    case class Bytes(bytesArray: Array[Byte], bytebuffer: ByteBuffer)

    val schema = AvroSchema[Bytes]

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, "testString1".getBytes)
      row.setField(1, "testString2".getBytes)
      row
    }

    val fromRowData: FromRowData[Bytes] = FromRowData.apply[Bytes](logicalType)

    val bytes = fromRowData.from(rowData)

    String(bytes.bytesArray) shouldBe "testString1"
    String(bytes.bytebuffer.array()) shouldBe "testString2"

  }

  it should "convert tuples" in {
    case class Tup(tuple: (String, String))
    val schema      = AvroSchema[Tup]
    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowTuple = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, StringData.fromString("A"))
      row.setField(1, StringData.fromString("3"))
      row
    }

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, rowTuple)
      row
    }
    val fromRowData: FromRowData[Tup] = FromRowData.apply[Tup](logicalType)
    val tup                           = fromRowData.from(rowData)

    tup.tuple shouldBe ("A", "3")
  }

  it should "convert temporal types" in {
    case class TimeAndDates(
        localDateTime: LocalDateTime,
        date: Date,
        instant: Instant,
        localDate: LocalDate,
        timestamp: Timestamp,
        offsetDateTime: OffsetDateTime,
        localTime: LocalTime
    )
    val schema = AvroSchema[TimeAndDates]

    val testInstant = Instant.now
    val testDate    = LocalDate.now
    val testOffsetDateTime =
      OffsetDateTime.ofInstant(testInstant.truncatedTo(ChronoUnit.MILLIS), ZoneOffset.UTC) // truncate to millis

    val logicalType = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 7)
      row.setField(0, TimestampData.fromInstant(testInstant).getMillisecond)
      row.setField(1, Int.box(testDate.toEpochDay.toInt))
      row.setField(2, TimestampData.fromInstant(testInstant))
      row.setField(3, LocalDate.ofInstant(testInstant, ZoneOffset.UTC).toEpochDay.toInt)
      row.setField(4, TimestampData.fromInstant(testInstant))
      row.setField(5, StringData.fromString(testOffsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
      // broken, since TIME_WITHOUT_TIMEZONE only stores integers  (see: org.apache.flink.table.data.RowData.createFieldGetter)
      row.setField(6, Int.box(LocalTime.MIDNIGHT.toNanoOfDay.toInt))
      row
    }

    val fromRowData: FromRowData[TimeAndDates] = FromRowData.apply[TimeAndDates](logicalType)

    val timesAndDates = fromRowData.from(rowData)

    timesAndDates.localDateTime shouldBe LocalDateTime
      .ofInstant(testInstant, ZoneOffset.UTC)
      .truncatedTo(ChronoUnit.MILLIS)
    timesAndDates.date shouldBe Date.valueOf(testDate)
    timesAndDates.instant shouldBe testInstant
    timesAndDates.localDate shouldBe testDate
    timesAndDates.timestamp shouldBe Timestamp.from(testInstant)
    timesAndDates.offsetDateTime.toInstant shouldBe testOffsetDateTime.toInstant // use of instant to have stable tests
    timesAndDates.localTime shouldBe LocalTime.MIDNIGHT

  }

  it should "convert BigInt to Instant" in {
    case class Test(id: Int, inst: Instant)

    // required because avro derivation uses timestampdata for inst
    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("id", INT()),
      DataTypes.FIELD("inst", BIGINT())
    )

    val testInst = Instant.now()

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(42)) // boxed Int, as Flink fields are Objects
      row.setField(1, Long.box(testInst.toEpochMilli))
      row
    }

    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](customType.getLogicalType)

    val test = fromRowData.from(rowData)

    test.inst shouldBe testInst.truncatedTo(ChronoUnit.MILLIS)

  }

  it should "use annotations" in {
    case class Test(@AvroName("ID_RENAMED") id: Int)
    val schema = AvroSchema[Test]
    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, Int.box(42))
      row
    }
    val logicalType                    = AvroSchemaConverter.convertToDataType(schema.toString).getLogicalType
    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](logicalType)
    val test                           = fromRowData.from(rowData)

    test.id shouldBe 42

  }
