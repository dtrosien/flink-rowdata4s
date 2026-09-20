package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.annotations.{TableChar, TableDecimal, TableName, TableVarchar}
import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.{DECIMAL, INT, MAP, MULTISET, STRING}
import org.apache.flink.table.data.{RowData, TimestampData}
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{BigIntType, CharType, DoubleType, FloatType, IntType, RowType, SmallIntType, TimestampType, TinyIntType, VarCharType}
import org.apache.flink.types.RowKind

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
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

  it should "fit Strings to the declared column length" in {
    val stringEncoder = Encoder[String]

    // VARCHAR(n) truncates, STRING (VARCHAR(MAX)) does not
    stringEncoder.encode(new VarCharType(3))("abcdef").toString shouldBe "abc"
    stringEncoder.encode(new VarCharType(3))("ab").toString shouldBe "ab"
    stringEncoder.encode(new VarCharType(VarCharType.MAX_LENGTH))("abcdef").toString shouldBe "abcdef"

    // CHAR(n) truncates or pads with spaces
    stringEncoder.encode(new CharType(5))("ab").toString shouldBe "ab   "
    stringEncoder.encode(new CharType(2))("abc").toString shouldBe "ab"

    // lengths count code points, not UTF-16 units
    stringEncoder.encode(new VarCharType(2))("\uD83D\uDE00\uD83D\uDE00\uD83D\uDE00").toString shouldBe "\uD83D\uDE00\uD83D\uDE00"
  }

  it should "reject a String field on a column that is not CHAR or VARCHAR" in {
    case class Test(name: String)
    val customType: DataType = DataTypes.ROW(DataTypes.FIELD("name", INT().notNull))

    an[UnsupportedOperationException] should be thrownBy ToRowData.apply[Test](customType.getLogicalType)
  }

  it should "truncate annotated String fields through the derived schema" in {
    case class Test(@TableVarchar(3) name: String, @TableChar(2) country: String)

    val logicalType                = FlinkDataType[Test].getLogicalType
    val toRowData: ToRowData[Test] = ToRowData.apply[Test](logicalType)

    val rowData = toRowData.to(Test("Alice", "D"))

    rowData.getString(0).toString shouldBe "Ali"
    rowData.getString(1).toString shouldBe "D "
    FromRowData.apply[Test](logicalType).from(rowData) shouldBe Test("Ali", "D ")
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

  it should "encode big decimals with the precision and scale of a @TableDecimal annotation" in {
    case class Deci(@TableDecimal(18, 4) amount: BigDecimal)
    val deci = Deci(BigDecimal("12345678901234.5"))

    val logicalType                = FlinkDataType[Deci].getLogicalType
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](logicalType)

    val rowData = toRowData.to(deci)

    rowData.getDecimal(0, 18, 4).toBigDecimal shouldBe new java.math.BigDecimal("12345678901234.5000")
    FromRowData.apply[Deci](logicalType).from(rowData) shouldBe Deci(BigDecimal("12345678901234.5000"))
  }

  it should "convert big decimals with the scale of the column" in {
    case class Deci(bigDecimal: BigDecimal)
    val deci = Deci(BigDecimal("1.5")) // scale 1, column has scale 2

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("bigDecimal", DECIMAL(10, 2).notNull)
    )
    val logicalType                = customType.getLogicalType
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](logicalType)

    val rowData = toRowData.to(deci)

    rowData.getDecimal(0, 10, 2).scale shouldBe 2

    // flink serializes the unscaled value and re-applies the column scale when reading
    val serializer = new RowDataSerializer(logicalType.asInstanceOf[RowType])
    val out        = new DataOutputSerializer(64)
    serializer.serialize(rowData, out)
    val deserialized: RowData = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer))

    deserialized.getDecimal(0, 10, 2).toBigDecimal shouldBe new java.math.BigDecimal("1.50")
    FromRowData.apply[Deci](logicalType).from(deserialized) shouldBe Deci(BigDecimal("1.50"))

  }

  it should "round big decimals with more fractional digits than the column" in {
    case class Deci(bigDecimal: BigDecimal)
    val deci = Deci(BigDecimal("1.005"))

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("bigDecimal", DECIMAL(10, 2).notNull)
    )
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](customType.getLogicalType)

    val rowData = toRowData.to(deci)

    rowData.getDecimal(0, 10, 2).toBigDecimal shouldBe new java.math.BigDecimal("1.01")

  }

  it should "throw on big decimals that do not fit the column precision" in {
    case class Deci(bigDecimal: BigDecimal)
    val deci = Deci(BigDecimal("123456789.123"))

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("bigDecimal", DECIMAL(10, 2).notNull)
    )
    val toRowData: ToRowData[Deci] = ToRowData.apply[Deci](customType.getLogicalType)

    an[IllegalArgumentException] should be thrownBy toRowData.to(deci)

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

  it should "convert java.util.Date like an Instant" in {
    case class WithUtilDate(d: java.util.Date)
    val instant = Instant.parse("2026-09-20T10:15:30.123Z")

    val logicalType                        = FlinkDataType[WithUtilDate].getLogicalType // TIMESTAMP_LTZ(3)
    val toRowData: ToRowData[WithUtilDate] = ToRowData.apply[WithUtilDate](logicalType)

    val rowData = toRowData.to(WithUtilDate(java.util.Date.from(instant)))

    rowData.getTimestamp(0, 3).toInstant shouldBe instant
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

    // checks: the derived schema uses precision 6 (TIMESTAMP for wall-clock values, TIMESTAMP_LTZ for instants),
    // which keeps the sub-millisecond part of the values
    rowData.getTimestamp(0, 6).toLocalDateTime shouldBe LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    rowData.getInt(1) shouldBe LocalDate.ofInstant(testInstant, ZoneOffset.UTC).toEpochDay
    rowData.getTimestamp(2, 6).toInstant shouldBe testInstant
    rowData.getInt(3) shouldBe LocalDate.ofInstant(testInstant, ZoneOffset.UTC).toEpochDay

    // flink timestamp is transformed to LocalDateTimeFirst before getting converted to Timestamp
    rowData.getTimestamp(4, 6).toTimestamp shouldBe Timestamp.valueOf(
      LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    )

    rowData.getInt(5) shouldBe (LocalTime.ofInstant(testInstant, ZoneOffset.UTC).toNanoOfDay / 1_000_000).toInt

    rowData.getTimestamp(6, 6).toInstant shouldBe testInstant

    // BIGINT columns take LocalDateTime as epoch millis, TIMESTAMP(3) truncates to millis
    val ldt = LocalDateTime.ofInstant(testInstant, ZoneOffset.UTC)
    Encoder[LocalDateTime].encode(new BigIntType())(ldt) shouldBe java.lang.Long.valueOf(testInstant.toEpochMilli)
    Encoder[LocalDateTime].encode(new TimestampType(false, 6))(ldt) shouldBe TimestampData.fromLocalDateTime(ldt)
    Encoder[LocalDateTime].encode(new TimestampType(false, 3))(ldt) shouldBe
      TimestampData.fromLocalDateTime(ldt.truncatedTo(ChronoUnit.MILLIS))

  }

  it should "convert rich enums as unions and read them back" in {
    enum Shape {
      case Circle(radius: Double)
      case Rect(width: Int, height: Int)
      case Unknown
    }
    case class Record(id: Int, shape: Shape)

    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)
    val fromRowData                  = FromRowData.apply[Record](logicalType)

    val circle = toRowData.to(Record(1, Shape.Circle(2.5)))
    circle.getRow(1, 3).getRow(0, 1).getDouble(0) shouldBe 2.5 // payload is kept
    circle.getRow(1, 3).isNullAt(1) shouldBe true
    circle.getRow(1, 3).isNullAt(2) shouldBe true

    for shape <- Seq(Shape.Circle(2.5), Shape.Rect(3, 4), Shape.Unknown) do
      fromRowData.from(toRowData.to(Record(1, shape))) shouldBe Record(1, shape)
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

  it should "write union fields by name when the schema lists the variants in another order" in {
    sealed trait Shape
    case class Circle(radius: Double)          extends Shape
    case class Rect(width: Int, height: Int)   extends Shape
    case class Record(id: Int, shape: Shape)

    // derived order is Circle, Rect; the table lists Rect first
    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("id", INT().notNull),
      DataTypes.FIELD(
        "shape",
        DataTypes
          .ROW(
            DataTypes.FIELD("Rect", DataTypes.ROW(DataTypes.FIELD("width", INT().notNull), DataTypes.FIELD("height", INT().notNull))),
            DataTypes.FIELD("Circle", DataTypes.ROW(DataTypes.FIELD("radius", DataTypes.DOUBLE().notNull)))
          )
          .notNull
      )
    )
    val logicalType                  = customType.getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    val rowData = toRowData.to(Record(1, Circle(2.5)))

    val union = rowData.getRow(1, 2)
    union.isNullAt(0) shouldBe true // Rect
    union.getRow(1, 1).getDouble(0) shouldBe 2.5 // Circle

    FromRowData.apply[Record](logicalType).from(rowData) shouldBe Record(1, Circle(2.5))
    FromRowData.apply[Record](logicalType).from(toRowData.to(Record(2, Rect(3, 4)))) shouldBe Record(2, Rect(3, 4))
  }

  it should "encode annotated enum cases with the annotated name, so the decoder finds them again" in {
    sealed trait Status
    @TableName("ACTIVE") case object Active extends Status
    case object Inactive                    extends Status
    case class Record(status: Status)

    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    toRowData.to(Record(Active)).getString(0).toString shouldBe "ACTIVE"
    toRowData.to(Record(Inactive)).getString(0).toString shouldBe "Inactive"
    FromRowData.apply[Record](logicalType).from(toRowData.to(Record(Active))) shouldBe Record(Active)
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

  it should "write columns in schema order, matching parameters by name" in {
    // parameter order: Int, String, String, Boolean
    case class User(id: Int, firstName: String, lastName: String, active: Boolean)
    val user = User(id = 42, firstName = "Alice", lastName = "Smith", active = true)

    // schema column order: Boolean, String, Int, String - every column sits at a different position than its
    // parameter, and the types at each position differ, so the encoder can only succeed by matching names
    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("active", DataTypes.BOOLEAN().notNull), // position 0, parameter 3
      DataTypes.FIELD("lastName", STRING().notNull),          // position 1, parameter 2
      DataTypes.FIELD("id", INT().notNull),                   // position 2, parameter 0
      DataTypes.FIELD("firstName", STRING().notNull)          // position 3, parameter 1
    )
    val toRowData: ToRowData[User] = ToRowData.apply[User](customType.getLogicalType)

    val rowData = toRowData.to(user)

    rowData.getBoolean(0) shouldBe true
    rowData.getString(1).toString shouldBe "Smith"
    rowData.getInt(2) shouldBe 42
    rowData.getString(3).toString shouldBe "Alice"
  }

  it should "carry the requested RowKind" in {
    case class Record(id: Int)
    val record = Record(42)

    val logicalType                  = FlinkDataType[Record].getLogicalType
    val toRowData: ToRowData[Record] = ToRowData.apply[Record](logicalType)

    toRowData.to(record).getRowKind shouldBe RowKind.INSERT
    toRowData.to(record, RowKind.UPDATE_BEFORE).getRowKind shouldBe RowKind.UPDATE_BEFORE
    toRowData.to(record, RowKind.UPDATE_AFTER).getRowKind shouldBe RowKind.UPDATE_AFTER
    toRowData.to(record, RowKind.DELETE).getRowKind shouldBe RowKind.DELETE

    // the kind does not change the payload
    toRowData.to(record, RowKind.DELETE).getInt(0) shouldBe 42
  }

  it should "convert Byte and Short primitives" in {
    val byteEncoder  = Encoder[Byte]
    val shortEncoder = Encoder[Short]

    byteEncoder.encode(new TinyIntType())(42.toByte) shouldBe java.lang.Byte.valueOf(42.toByte)
    shortEncoder.encode(new SmallIntType())(100.toShort) shouldBe java.lang.Short.valueOf(100.toShort)
  }

  it should "widen Byte and Short to the INT column of the derived schema" in {
    case class SmallPrimitives(b: Byte, s: Short)
    val smallPrimitives = SmallPrimitives(1, 2)

    val logicalType                           = FlinkDataType[SmallPrimitives].getLogicalType
    val toRowData: ToRowData[SmallPrimitives] = ToRowData.apply[SmallPrimitives](logicalType)

    val rowData = toRowData.to(smallPrimitives)

    rowData.getInt(0) shouldBe 1
    rowData.getInt(1) shouldBe 2

    // the serializer only accepts the column's java type
    val serializer = new RowDataSerializer(logicalType.asInstanceOf[RowType])
    val out        = new DataOutputSerializer(64)
    serializer.serialize(rowData, out)
    val deserialized: RowData = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer))

    FromRowData.apply[SmallPrimitives](logicalType).from(deserialized) shouldBe smallPrimitives

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
    // MULTISET<STRING> is a map from element to count
    case class WithMultiset(counts: Map[String, Int])

    val customType: DataType = DataTypes.ROW(
      DataTypes.FIELD("counts", MULTISET(STRING().notNull).notNull)
    )
    val logicalType                        = customType.getLogicalType
    val toRowData: ToRowData[WithMultiset] = ToRowData.apply[WithMultiset](logicalType)

    val rowData = toRowData.to(WithMultiset(Map("a" -> 2)))

    rowData.getMap(0).keyArray().getString(0).toString shouldBe "a"
    rowData.getMap(0).valueArray().getInt(0) shouldBe 2

    // flink's own serializer for MULTISET expects exactly this layout
    val serializer = new RowDataSerializer(logicalType.asInstanceOf[RowType])
    val out        = new DataOutputSerializer(64)
    serializer.serialize(rowData, out)
    val deserialized: RowData = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer))

    FromRowData.apply[WithMultiset](logicalType).from(deserialized) shouldBe WithMultiset(Map("a" -> 2))
  }

  it should "support Encoder.identity" in {
    val enc    = Encoder.identity[String]
    val result = enc.encode(null)("hello")
    result shouldBe "hello"
  }

  it should "support Encoder.contramap" in {
    val intEncoder = Encoder[Int]
    val longToInt  = intEncoder.contramap[Long](_.toInt)
    val result     = longToInt.encode(new IntType())(42L)
    result shouldBe Integer.valueOf(42)
  }

  it should "truncate timestamps to milliseconds for columns with precision <= 3" in {
    case class Ts(instant: Instant)
    val ts = Ts(Instant.ofEpochSecond(1, 123456789))

    val millisType: DataType = DataTypes.ROW(DataTypes.FIELD("instant", DataTypes.TIMESTAMP(3).notNull))
    val nanosType: DataType  = DataTypes.ROW(DataTypes.FIELD("instant", DataTypes.TIMESTAMP(9).notNull))

    val millisRow = ToRowData.apply[Ts](millisType.getLogicalType).to(ts)
    val nanosRow  = ToRowData.apply[Ts](nanosType.getLogicalType).to(ts)

    // the serializer for TIMESTAMP(3) asserts that no nano-of-millisecond part is present
    millisRow.getTimestamp(0, 3).getNanoOfMillisecond shouldBe 0
    millisRow.getTimestamp(0, 3).toInstant shouldBe Instant.ofEpochSecond(1, 123000000)
    nanosRow.getTimestamp(0, 9).toInstant shouldBe ts.instant
  }

  it should "convert Float and Double to the floating point type of the column" in {
    val floatEncoder  = Encoder[Float]
    val doubleEncoder = Encoder[Double]

    floatEncoder.encode(new FloatType())(1.5f) shouldBe java.lang.Float.valueOf(1.5f)
    floatEncoder.encode(new DoubleType())(1.5f) shouldBe java.lang.Double.valueOf(1.5d)
    doubleEncoder.encode(new DoubleType())(1.5d) shouldBe java.lang.Double.valueOf(1.5d)
    doubleEncoder.encode(new FloatType())(1.5d) shouldBe java.lang.Float.valueOf(1.5f)
    an[UnsupportedOperationException] should be thrownBy doubleEncoder.encode(new IntType())
  }

  it should "convert Int and Long to the integer type of the column" in {
    val intEncoder  = Encoder[Int]
    val longEncoder = Encoder[Long]

    intEncoder.encode(new BigIntType())(42) shouldBe java.lang.Long.valueOf(42L)
    intEncoder.encode(new SmallIntType())(70000) shouldBe java.lang.Short.valueOf(70000.toShort) // wraps around
    longEncoder.encode(new IntType())(42L) shouldBe Integer.valueOf(42)
    longEncoder.encode(new IntType())(1L << 33 | 5) shouldBe Integer.valueOf(5) // wraps around
    an[UnsupportedOperationException] should be thrownBy longEncoder.encode(new TimestampType(3))
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
