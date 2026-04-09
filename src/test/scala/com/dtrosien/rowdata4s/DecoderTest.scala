package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.annotations.TableName
import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.{BIGINT, INT, STRING, VARCHAR}
import org.apache.flink.table.types.logical.MultisetType
import org.apache.flink.table.data.*
import org.apache.flink.table.types.DataType
import org.apache.flink.types.RowKind

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.jdk.CollectionConverters.*

class DecoderTest extends UnitSpec:

  "Decoder" should "convert Primitives" in {
    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)

    val logicalType = FlinkDataType[Primitives].getLogicalType

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

    val logicalType = FlinkDataType[Optionals].getLogicalType

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

    val logicalType = FlinkDataType[TestNested].getLogicalType

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

    val logicalType = FlinkDataType[Strings].getLogicalType

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
        vec: Vector[String],
        set: Set[String]
    )

    val logicalType = FlinkDataType[Collections].getLogicalType

    val innerRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, StringData.fromString("testString"))
      row
    }
    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 6)
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
      row.setField(5, GenericArrayData(Array[Object](StringData.fromString("a"), StringData.fromString("b"))))
      row
    }

    val fromRowData: FromRowData[Collections] = FromRowData.apply[Collections](logicalType)

    val collections = fromRowData.from(rowData)

    collections.list shouldBe List(true, false, true)
    collections.map shouldBe Map("1" -> Inner("testString"), "2" -> Inner("testString"))
    collections.arr shouldBe Array(None, Some(Inner("testString")), None)
    collections.seq shouldBe Seq(1, 2, 3)
    collections.vec shouldBe Vector("1", "2", "3")
    collections.set shouldBe Set("a", "b")

  }

  it should "convert big decimal" in {
    case class Decimal(dec: BigDecimal)

    val logicalType = FlinkDataType[Decimal].getLogicalType

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

    val logicalType = FlinkDataType[Bytes].getLogicalType

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
    val logicalType = FlinkDataType[Tup].getLogicalType

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

    val testInstant = Instant.now
    val testDate    = LocalDate.now
    val testOffsetDateTime =
      OffsetDateTime.ofInstant(testInstant.truncatedTo(ChronoUnit.MILLIS), ZoneOffset.UTC) // truncate to millis

    val logicalType = FlinkDataType[TimeAndDates].getLogicalType

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
    case class Test(@TableName("ID_RENAMED") id: Int)

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, Int.box(42))
      row
    }
    val logicalType                    = FlinkDataType[Test].getLogicalType
    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](logicalType)
    val test                           = fromRowData.from(rowData)

    test.id shouldBe 42

  }

  it should "convert simple enums" in {
    enum Enum {
      case ABC, CBA
    }

    case class Test(id: Int, en: Enum)

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(42))
      row.setField(1, StringData.fromString("ABC"))
      row
    }

    val logicalType                    = FlinkDataType[Test].getLogicalType
    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](logicalType)

    val test = fromRowData.from(rowData)

    test.en shouldBe Enum.ABC
  }

  it should "convert sealed traits with objects" in {
    sealed trait SealedTrait
    case object ABC extends SealedTrait
    case object CBA extends SealedTrait

    case class Test(id: Int, st: SealedTrait)

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(42))
      row.setField(1, StringData.fromString("ABC"))
      row
    }

    val logicalType                    = FlinkDataType[Test].getLogicalType
    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](logicalType)

    val test = fromRowData.from(rowData)

    test.st shouldBe ABC
  }

  it should "convert sealed traits with classes" in {
    sealed trait SealedTrait
    case class A(a: String)         extends SealedTrait
    case class B(a: String, b: Int) extends SealedTrait

    case class Test(id: Int, st: SealedTrait)

    val valueRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, StringData.fromString("ABC"))
      row.setField(1, Int.box(123))
      row
    }

    val enumRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, null)
      row.setField(1, valueRow)
      row
    }

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(42))
      row.setField(1, enumRow)
      row
    }

    val logicalType                    = FlinkDataType[Test].getLogicalType
    val fromRowData: FromRowData[Test] = FromRowData.apply[Test](logicalType)
    val test = fromRowData.from(rowData)

    test.st shouldBe B("ABC", 123)
  }

  it should "convert Byte and Short primitives" in {
    case class SmallPrimitives(b: Byte, s: Short)

    val logicalType = FlinkDataType[SmallPrimitives].getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(1))
      row.setField(1, Int.box(2))
      row
    }

    val fromRowData = FromRowData.apply[SmallPrimitives](logicalType)
    val result      = fromRowData.from(rowData)

    result.b shouldBe 1.toByte
    result.s shouldBe 2.toShort
  }

  it should "convert byte iterables" in {
    case class ByteIterables(listBytes: List[Byte], seqBytes: Seq[Byte], vecBytes: Vector[Byte])

    val logicalType = FlinkDataType[ByteIterables].getLogicalType
    val bytes       = "hello".getBytes

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 3)
      row.setField(0, bytes)
      row.setField(1, bytes)
      row.setField(2, bytes)
      row
    }

    val fromRowData = FromRowData.apply[ByteIterables](logicalType)
    val result      = fromRowData.from(rowData)

    result.listBytes shouldBe bytes.toList
    result.seqBytes shouldBe bytes.toSeq
    result.vecBytes shouldBe bytes.toVector
  }

  it should "convert BigDecimal from String" in {
    case class Decimal(dec: BigDecimal)

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("dec", STRING().notNull)
    )

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, StringData.fromString("123.45"))
      row
    }

    val fromRowData = FromRowData.apply[Decimal](customType.getLogicalType)
    val result      = fromRowData.from(rowData)

    result.dec shouldBe BigDecimal("123.45")
  }

  it should "convert ByteBuffer from ByteBuffer input" in {
    // ByteBufferDecoder handles ByteBuffer directly (bypassing FieldGetter which expects byte[])
    val bytes   = "world".getBytes
    val buf     = ByteBuffer.wrap(bytes)
    val decoder = Decoder[ByteBuffer]
    val result  = decoder.decode(FlinkDataType[ByteBuffer].getLogicalType)(buf)

    result shouldBe buf
  }

  it should "decode String from plain String and CharSequence values" in {
    // StringDecoder and CharSequenceDecoder accept raw String/CharSequence (not only StringData)
    val stringDecoder = Decoder[String]
    val csDecoder     = Decoder[CharSequence]
    val logicalType   = FlinkDataType[String].getLogicalType

    stringDecoder.decode(logicalType)("plain") shouldBe "plain"
    csDecoder.decode(logicalType)(new StringBuilder("chars")).toString shouldBe "chars"
  }

  it should "use default value when field fails to decode" in {
    case class WithDefault(id: Int, name: String = "fallback")

    val logicalType = FlinkDataType[WithDefault].getLogicalType

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, Int.box(7))
      row.setField(1, null) // null causes StringDecoder to throw; default kicks in
      row
    }

    val fromRowData = FromRowData.apply[WithDefault](logicalType)
    val result      = fromRowData.from(rowData)

    result.id shouldBe 7
    result.name shouldBe "fallback"
  }

  it should "support Decoder.map" in {
    val intDecoder    = Decoder[Int]
    val stringDecoder = intDecoder.map(_.toString)
    val result        = stringDecoder.decode(null)(42)
    result shouldBe "42"
  }

  it should "decode primitives from alternative input types" in {
    // Byte decoder – actual Byte input
    Decoder[Byte].decode(null)(42.toByte) shouldBe 42.toByte

    // Short decoder – Byte and Short inputs
    Decoder[Short].decode(null)(1.toByte) shouldBe 1.toShort
    Decoder[Short].decode(null)(2.toShort) shouldBe 2.toShort

    // Int decoder – Byte and Short inputs
    Decoder[Int].decode(null)(1.toByte) shouldBe 1
    Decoder[Int].decode(null)(2.toShort) shouldBe 2

    // Long decoder – Byte, Short, and Int inputs
    Decoder[Long].decode(null)(1.toByte) shouldBe 1L
    Decoder[Long].decode(null)(2.toShort) shouldBe 2L
    Decoder[Long].decode(null)(3) shouldBe 3L

    // Double decoder – boxed java.lang.Double input
    Decoder[Double].decode(null)(java.lang.Double.valueOf(1.5)) shouldBe 1.5

    // Float decoder – boxed java.lang.Float input
    Decoder[Float].decode(null)(java.lang.Float.valueOf(1.5f)) shouldBe 1.5f
  }

  it should "throw on unsupported primitive inputs" in {
    an[UnsupportedOperationException] should be thrownBy Decoder[Int].decode(null)("bad")
    an[UnsupportedOperationException] should be thrownBy Decoder[Long].decode(null)("bad")
  }

  it should "decode collections from alternative input types" in {
    val logicalType = FlinkDataType[List[Int]].getLogicalType

    // java.util.Collection input
    val javaList = new java.util.ArrayList[Int]()
    javaList.add(1)
    javaList.add(2)
    Decoder[List[Int]].decode(logicalType)(javaList) shouldBe List(1, 2)

    // plain Scala Array input
    Decoder[List[Int]].decode(logicalType)(Array(3, 4)) shouldBe List(3, 4)

    // plain Scala Iterable input
    Decoder[List[Int]].decode(logicalType)(Iterable(5, 6)) shouldBe List(5, 6)
  }

  it should "decode Array from alternative input types" in {
    val logicalType = FlinkDataType[Array[Int]].getLogicalType

    // java.util.Collection input
    val javaList = new java.util.ArrayList[Int]()
    javaList.add(10)
    javaList.add(20)
    Decoder[Array[Int]].decode(logicalType)(javaList) shouldBe Array(10, 20)

    // plain Scala Iterable input
    Decoder[Array[Int]].decode(logicalType)(Iterable(30, 40)) shouldBe Array(30, 40)

    // plain Scala Array input
    Decoder[Array[Int]].decode(logicalType)(Array(50, 60)) shouldBe Array(50, 60)
  }

  it should "decode Map from MULTISET logical type" in {
    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("counts", DataTypes.MULTISET(STRING().notNull).notNull)
    )
    case class WithMultiset(counts: Map[Int, String])

    val keyRow = GenericMapData(
      Map(Int.box(1) -> StringData.fromString("a")).asJava
    )

    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, keyRow)
      row
    }

    val fromRowData = FromRowData.apply[WithMultiset](customType.getLogicalType)
    val result      = fromRowData.from(rowData)

    result.counts shouldBe Map(1 -> "a")
  }

  it should "decode Map from java.util.Map input" in {
    val logicalType = FlinkDataType[Map[String, Int]].getLogicalType

    val javaMap = new java.util.HashMap[Any, Any]()
    javaMap.put(StringData.fromString("x"), 99)

    Decoder[Map[String, Int]].decode(logicalType)(javaMap) shouldBe Map("x" -> 99)
  }

  it should "decode Array[Byte] from ByteBuffer input" in {
    val bytes  = "hello".getBytes
    val result = ArrayByteDecoder.decode(null)(ByteBuffer.wrap(bytes))
    result shouldBe bytes
  }

  it should "throw on unsupported ArrayByteDecoder input" in {
    an[UnsupportedOperationException] should be thrownBy ArrayByteDecoder.decode(null)("bad")
  }

  it should "throw on unsupported ByteBufferDecoder input" in {
    an[UnsupportedOperationException] should be thrownBy ByteBufferDecoder.decode(null)("bad")
  }

  it should "decode Instant from Int epoch millis" in {
    val epochMillis = 1_000_000
    val result      = Decoder[Instant].decode(null)(epochMillis)
    result shouldBe Instant.ofEpochMilli(epochMillis.toLong)
  }

  it should "throw when all fields are null in a union ROW" in {
    sealed trait St
    case class A(a: String) extends St
    case class B(b: Int)    extends St
    case class Test(st: St)

    val logicalType = FlinkDataType[Test].getLogicalType

    val unionRow: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 2)
      row.setField(0, null)
      row.setField(1, null)
      row
    }
    val rowData: RowData = {
      val row = new GenericRowData(RowKind.INSERT, 1)
      row.setField(0, unionRow)
      row
    }

    a[RuntimeException] should be thrownBy FromRowData.apply[Test](logicalType).from(rowData)
  }
