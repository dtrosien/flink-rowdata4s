package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.{INT, MAP, MULTISET, STRING}
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.TimestampType

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.jdk.CollectionConverters.*

class EncoderTest extends UnitSpec:

  "Encoder" should "convert primitives" in {

    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)

    val logicalType = FlinkDataType[Primitives].getLogicalType

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

    val logicalType = FlinkDataType[Optionals].getLogicalType

    val optionals = Optionals(Some(42), None)

    val toRowData: ToRowData[Optionals] = ToRowData.apply[Optionals](logicalType)

    val rowData = toRowData.to(optionals)

    rowData.getInt(0) shouldBe 42
    rowData.getString(1) shouldBe null
  }

  it should "convert nested data" in {
    case class Inner(name: String)
    case class Outer(id: Int, inner: Inner)

    val logicalType = FlinkDataType[Outer].getLogicalType

    val outer = Outer(42, Inner("someName"))

    val toRowData: ToRowData[Outer] = ToRowData.apply[Outer](logicalType)

    val rowData = toRowData.to(outer)

    rowData.getRow(1, 1).getString(0).toString shouldBe "someName"

  }

  it should "convert String" in {

    case class Strings(uuid: UUID, str: String, charSequence: CharSequence)
    val logicalType = FlinkDataType[Strings].getLogicalType

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
        vec: Vector[String],
        set: Set[String]
    )

    val inner = Inner("someName")
    val collections = Collections(
      List(true, false),
      Map("key" -> inner),
      Array(Some(inner), None, Some(inner)),
      Seq(1, 2),
      Vector("1", "2"),
      Set("a")
    )

    val logicalType                       = FlinkDataType[Collections].getLogicalType
    val toRowData: ToRowData[Collections] = ToRowData.apply[Collections](logicalType)

    val rowData = toRowData.to(collections)

    rowData.getArray(0).getBoolean(1) shouldBe false
    rowData.getMap(1).valueArray().getRow(0, 1).getString(0).toString shouldBe "someName"
    rowData.getArray(2).getRow(1, 1) shouldBe null
    rowData.getArray(3).getInt(1) shouldBe 2
    rowData.getArray(4).getString(0).toString shouldBe "1"
    rowData.getArray(5).getString(0).toString shouldBe "a"
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
    val deci                       = Deci(BigDecimal.valueOf(123))
    val logicalType                = FlinkDataType[Deci].getLogicalType
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](logicalType)

    val rowData = toRowData.to(deci)

    rowData.getDecimal(0, 8, 2).toBigDecimal.longValue() shouldBe 123L

  }

  it should "convert bytes" in {
    case class Bytes(bytes: Array[Byte], bytebuffer: ByteBuffer)
    val logicalType = FlinkDataType[Bytes].getLogicalType

    val bytes = Bytes("asas".getBytes, ByteBuffer.wrap("asas".getBytes))

    val toRowData: ToRowData[Bytes] = ToRowData.apply[Bytes](logicalType)

    val rowData = toRowData.to(bytes)

    rowData.getBinary(0) shouldBe "asas".getBytes
    rowData.getBinary(1) shouldBe "asas".getBytes
  }

  it should "convert tuples" in {
    case class Tup(tuple: (String, String))
    val logicalType               = FlinkDataType[Tup].getLogicalType
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

    val logicalType = FlinkDataType[TimeAndDates].getLogicalType

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

    rowData.getInt(5) shouldBe (LocalTime.ofInstant(testInstant, ZoneOffset.UTC).toNanoOfDay / 1_000_000).toInt

    OffsetDateTime
      .parse(rowData.getString(6).toString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
      .toInstant shouldBe OffsetDateTime
      .ofInstant(testInstant, ZoneOffset.UTC)
      .toInstant // use of instant to have stable tests

    // TIMESTAMP_WITHOUT_TIME_ZONE branch (FlinkDataType uses BIGINT for LocalDateTime by default)
    val ldt = LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    Encoder[LocalDateTime].encode(new TimestampType(false, 3))(ldt) shouldBe TimestampData.fromLocalDateTime(ldt)

  }

  it should "convert enums" in {
    enum Enum {
      case ABC, CBA
    }
    case class Record(en: Enum)

    val en                           = Enum.ABC
    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rowData = toRowData.to(Record(en))

    rowData.getString(0).toString shouldBe "ABC"
  }

  it should "convert sealed traits with case objects" in {
    sealed trait Enum
    case object ABC extends Enum
    case object CBA extends Enum
    case class Record(en: Enum)

    val rec                          = Record(ABC)
    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rowData = toRowData.to(rec)

    rowData.getString(0).toString shouldBe "ABC"
  }

  it should "convert sealed traits with single instance directly" in {
    sealed trait TestSealedTrait
    case class Test1(a: String, b: Int) extends TestSealedTrait
    case class Record(st: TestSealedTrait)

    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rec = Record(Test1(a = "ABC", b = 123))

    val rowData = toRowData.to(rec)

    rowData.getRow(0, 1).getString(0).toString shouldBe "ABC"
    rowData.getRow(0, 1).getInt(1) shouldBe 123
  }

  it should "convert sealed traits" in {
    sealed trait TestSealedTrait
    case class Test1(a: String) extends TestSealedTrait
    case class Test2(b: Inner)  extends TestSealedTrait
    case class Test3(c: Double) extends TestSealedTrait
    case class Inner(i: Int)

    case class Record(st: TestSealedTrait)
    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rec     = Record(Test2(b = Inner(123)))
    val rowData = toRowData.to(rec)

//    println(rowData)
//    println(logicalType)
    rowData.getRow(0, 1).getRow(1, 3).getRow(0, 1).getInt(0) shouldBe 123

  }

  it should "convert objects" in {
    case object SomeObject
    case class Record(obj: SomeObject.type)
    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rec = Record(SomeObject)

    val rowData = toRowData.to(rec)

    rowData.getString(0).toString shouldBe "SomeObject"
  }

  it should "convert Byte and Short primitives" in {
    val byteEncoder  = Encoder[Byte]
    val shortEncoder = Encoder[Short]

    byteEncoder.encode(null)(42.toByte) shouldBe java.lang.Byte.valueOf(42.toByte)
    shortEncoder.encode(null)(100.toShort) shouldBe java.lang.Short.valueOf(100.toShort)
  }

  it should "convert byte iterables" in {
    case class ByteIterables(listBytes: List[Byte], seqBytes: Seq[Byte], vecBytes: Vector[Byte])

    val logicalType                         = FlinkDataType[ByteIterables].getLogicalType
    val toRowData: ToRowData[ByteIterables] = ToRowData.apply[ByteIterables](logicalType)

    val data    = "hello".getBytes.toList
    val rowData = toRowData.to(ByteIterables(data, data.toSeq, data.toVector))

    rowData.getBinary(0) shouldBe "hello".getBytes
    rowData.getBinary(1) shouldBe "hello".getBytes
    rowData.getBinary(2) shouldBe "hello".getBytes
  }

  it should "convert Map with MULTISET type" in {
    case class WithMultiset(counts: Map[Int, String])

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("counts", MULTISET(STRING().notNull).notNull)
    )
    val logicalType                        = customType.getLogicalType
    val toRowData: ToRowData[WithMultiset] = ToRowData.apply[WithMultiset](logicalType)

    val rowData = toRowData.to(WithMultiset(Map(1 -> "a")))

    rowData.getMap(0).keyArray().getInt(0) shouldBe 1
    rowData.getMap(0).valueArray().getString(0).toString shouldBe "a"
  }

  it should "support Encoder.identity" in {
    val enc    = Encoder.identity[String]
    val result = enc.encode(null)("hello")
    result shouldBe "hello"
  }

  it should "support Encoder.contramap" in {
    val intEncoder = Encoder[Int]
    val longToInt  = intEncoder.contramap[Long](_.toInt)
    val result     = longToInt.encode(null)(42L)
    result shouldBe Integer.valueOf(42)
  }

  it should "throw on unsupported UUID schema type" in {
    import org.apache.flink.table.types.logical.IntType
    an[UnsupportedOperationException] should be thrownBy {
      UUIDEncoder.encode(new IntType())(UUID.randomUUID())
    }
  }

  it should "throw on unsupported ByteArrayEncoder schema type" in {
    import org.apache.flink.table.types.logical.VarCharType
    an[UnsupportedOperationException] should be thrownBy {
      ByteArrayEncoder.encode(new VarCharType())("bad".getBytes)
    }
  }

  it should "throw on unsupported ByteBufferEncoder schema type" in {
    import org.apache.flink.table.types.logical.VarCharType
    an[UnsupportedOperationException] should be thrownBy {
      ByteBufferEncoder.encode(new VarCharType())(ByteBuffer.wrap("bad".getBytes))
    }
  }

  it should "throw on unsupported MapEncoder schema type" in {
    import org.apache.flink.table.types.logical.IntType
    an[UnsupportedOperationException] should be thrownBy {
      Encoder[Map[String, Int]].encode(new IntType())(Map("a" -> 1))
    }
  }

  it should "throw on unsupported LocalDateTimeEncoder schema type" in {
    import org.apache.flink.table.types.logical.VarCharType
    an[UnsupportedOperationException] should be thrownBy {
      Encoder[LocalDateTime].encode(new VarCharType())(LocalDateTime.now())
    }
  }
