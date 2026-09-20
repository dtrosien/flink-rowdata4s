package com.dtrosien.rowdata4s.datatype

import com.dtrosien.rowdata4s.UnitSpec
import com.dtrosien.rowdata4s.annotations.{TableChar, TableComment, TableDecimal, TableName, TableTimestampPrecision, TableTransient, TableVarchar}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.DataTypes.*

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.util.UUID

class DatatypesTest extends UnitSpec:

  "DatatypesFor" should "derive Primitives" in {
    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)
    val dataType = FlinkDataType[Primitives]

    dataType shouldBe ROW(
      FIELD("int", INT.notNull),
      FIELD("long", BIGINT.notNull),
      FIELD("double", DOUBLE.notNull),
      FIELD("float", FLOAT.notNull),
      FIELD("boolean", BOOLEAN.notNull)
    ).notNull
  }

  it should "derive Optionals" in {
    case class Optionals(maybeInt: Option[Int], maybeString: Option[String])
    val dataType = FlinkDataType[Optionals]

    dataType shouldBe ROW(
      FIELD("maybeInt", INT.nullable),
      FIELD("maybeString", STRING.nullable)
    ).notNull
  }

  it should "derive Strings" in {
    case class Strings(uuid: UUID, str: String, charSequence: CharSequence)
    val dataType = FlinkDataType[Strings]

    dataType shouldBe ROW(
      FIELD("uuid", STRING.notNull),
      FIELD("str", STRING.notNull),
      FIELD("charSequence", STRING.notNull)
    ).notNull
  }

  it should "derive nested data" in {
    case class Test(id: Int, num: Long)
    case class TestNested(id: Int, num: Long, inner: Test)
    val dataType = FlinkDataType[TestNested]

    dataType shouldBe ROW(
      FIELD("id", INT.notNull),
      FIELD("num", BIGINT.notNull),
      FIELD("inner", ROW(FIELD("id", INT.notNull), FIELD("num", BIGINT.notNull)).notNull)
    ).notNull
  }

  it should "derive Collections" in {
    case class Inner(name: String)
    case class Collections(
        list: List[Boolean],
        map: Map[String, Inner],
        arr: Array[Option[Inner]],
        seq: Seq[Int],
        vec: Vector[String],
        set: Set[Int]
    )
    val dataType = FlinkDataType[Collections]

    val inner = ROW(FIELD("name", STRING.notNull))
    dataType shouldBe ROW(
      FIELD("list", ARRAY(BOOLEAN.notNull).notNull),
      FIELD("map", MAP(STRING.notNull, inner.notNull).notNull),
      FIELD("arr", ARRAY(inner.nullable).notNull),
      FIELD("seq", ARRAY(INT.notNull).notNull),
      FIELD("vec", ARRAY(STRING.notNull).notNull),
      FIELD("set", ARRAY(INT.notNull).notNull)
    ).notNull
  }

  it should "derive BigDecimal" in {
    case class Decimal(dec: BigDecimal)
    val dataType = FlinkDataType[Decimal]

    // precision and scale come from the ScalePrecision given, default (8, 2)
    dataType shouldBe ROW(FIELD("dec", DECIMAL(8, 2).notNull)).notNull
  }

  it should "derive Bytes" in {
    case class Bytes(bytesArray: Array[Byte], bytebuffer: ByteBuffer)
    val dataType = FlinkDataType[Bytes]

    dataType shouldBe ROW(
      FIELD("bytesArray", BYTES.notNull),
      FIELD("bytebuffer", BYTES.notNull)
    ).notNull
  }

  it should "derive Tuples" in {
    case class Tup(tuple: (String, String))
    val dataType = FlinkDataType[Tup]

    dataType shouldBe ROW(
      FIELD("tuple", ROW(FIELD("_1", STRING.notNull), FIELD("_2", STRING.notNull)).notNull)
    ).notNull
  }

  it should "derive temporal types" in {
    case class TimeAndDates(
        localDateTime: LocalDateTime,
        date: Date,
        instant: Instant,
        localDate: LocalDate,
        timestamp: Timestamp,
        offsetDateTime: OffsetDateTime,
        localTime: LocalTime
    )
    val dataType = FlinkDataType[TimeAndDates]

    // instants map to TIMESTAMP_LTZ, wall-clock values to TIMESTAMP, both with Flink's default precision 6
    dataType shouldBe DataTypes.ROW(
      DataTypes.FIELD("localDateTime", DataTypes.TIMESTAMP(6).notNull),
      DataTypes.FIELD("date", DataTypes.DATE.notNull),
      DataTypes.FIELD("instant", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull),
      DataTypes.FIELD("localDate", DataTypes.DATE.notNull),
      DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(6).notNull),
      DataTypes.FIELD("offsetDateTime", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull),
      DataTypes.FIELD("localTime", DataTypes.TIME(3).notNull)
    ).notNull
  }

  it should "derive java.util.Date" in {
    case class WithUtilDate(d: java.util.Date)
    val dataType = FlinkDataType[WithUtilDate]

    // java.util.Date behaves like an Instant and maps to TIMESTAMP_LTZ
    dataType shouldBe DataTypes.ROW(
      DataTypes.FIELD("d", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull)
    ).notNull
  }

  it should "derive timestamps with a custom precision" in {
    case class WithInstant(i: Instant, ldt: LocalDateTime)
    given TimestampPrecision = TimestampPrecision(3)
    val dataType             = FlinkDataType[WithInstant]

    dataType shouldBe DataTypes.ROW(
      DataTypes.FIELD("i", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull),
      DataTypes.FIELD("ldt", DataTypes.TIMESTAMP(3).notNull)
    ).notNull
  }

  it should "use Annotations" in {
    case class Test(@TableName("ID_RENAMED") id: Int, @TableTransient id2: Int)
    val dataType = FlinkDataType[Test]

    // the field is renamed, the transient field is left out
    dataType shouldBe ROW(FIELD("ID_RENAMED", INT.notNull)).notNull
  }

  it should "use field annotations for decimal precision, timestamp precision and comments" in {
    case class Test(
        @TableDecimal(18, 4) amount: BigDecimal,
        @TableDecimal(5, 2) rate: Option[BigDecimal],
        @TableTimestampPrecision(3) at: Instant,
        @TableTimestampPrecision(9) @TableComment("wall-clock time of the event") happenedAt: LocalDateTime,
        @TableComment("the id") id: Int
    )
    val dataType = FlinkDataType[Test]

    dataType shouldBe ROW(
      FIELD("amount", DECIMAL(18, 4).notNull),
      FIELD("rate", DECIMAL(5, 2).nullable),
      FIELD("at", TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull),
      FIELD("happenedAt", TIMESTAMP(9).notNull, "wall-clock time of the event"),
      FIELD("id", INT.notNull, "the id")
    ).notNull
  }

  it should "use field annotations for string lengths" in {
    case class Test(@TableVarchar(50) name: String, @TableChar(2) country: Option[String], @TableVarchar(36) id: UUID)
    val dataType = FlinkDataType[Test]

    dataType shouldBe ROW(
      FIELD("name", VARCHAR(50).notNull),
      FIELD("country", CHAR(2).nullable),
      FIELD("id", VARCHAR(36).notNull)
    ).notNull
  }

  it should "reject decimal, timestamp and string annotations on fields of another type" in {
    case class WrongDecimal(@TableDecimal(5, 2) id: Int)
    case class WrongTimestamp(@TableTimestampPrecision(3) name: String)
    case class WrongVarchar(@TableVarchar(5) id: Int)

    an[IllegalArgumentException] should be thrownBy FlinkDataType[WrongDecimal]
    an[IllegalArgumentException] should be thrownBy FlinkDataType[WrongTimestamp]
    an[IllegalArgumentException] should be thrownBy FlinkDataType[WrongVarchar]
  }

  it should "derive enum types" in {
    sealed trait SealedTrait
    case object Test1 extends SealedTrait
    case object Test2 extends SealedTrait

    enum Enum {
      case A, B
    }

    // enums and sealed traits of case objects are encoded as their name
    FlinkDataType[SealedTrait] shouldBe STRING.notNull
    FlinkDataType[Enum] shouldBe STRING.notNull
  }

  it should "derive sealed traits" in {
    case class Inner(i: Int)
    sealed trait InnerSealedTrait
    case class InnerSt1(d: String)                       extends InnerSealedTrait
    @TableName("INNER_2") case class InnerSt2(d: String) extends InnerSealedTrait

    sealed trait TestSealedTrait
    @TableName("TEST_1") case class Test1(@TableName("A") a: String) extends TestSealedTrait
    case class Test2(b: Int)                                         extends TestSealedTrait
    case class Test3(c: Double, i: Inner)                            extends TestSealedTrait
    case class Test4(c: Double, innerSt: InnerSealedTrait)           extends TestSealedTrait
    val dataType = FlinkDataType[TestSealedTrait]

    // one nullable field per variant, in class name order, named after the (annotated) class; nested sealed traits
    // nest the same way
    val innerSt = ROW(
      FIELD("InnerSt1", ROW(FIELD("d", STRING.notNull)).nullable),
      FIELD("INNER_2", ROW(FIELD("d", STRING.notNull)).nullable)
    ).notNull
    dataType shouldBe ROW(
      FIELD("TEST_1", ROW(FIELD("A", STRING.notNull)).nullable),
      FIELD("Test2", ROW(FIELD("b", INT.notNull)).nullable),
      FIELD("Test3", ROW(FIELD("c", DOUBLE.notNull), FIELD("i", ROW(FIELD("i", INT.notNull)).notNull)).nullable),
      FIELD("Test4", ROW(FIELD("c", DOUBLE.notNull), FIELD("innerSt", innerSt)).nullable)
    ).notNull
  }

  it should "derive rich enum types" in {
    enum Enum {
      case A(a: String)
      case B(a: String, b: Int)
    }

    val dataTypeEn = FlinkDataType[Enum]

    // enum cases with parameters are a union like a sealed trait of case classes
    dataTypeEn shouldBe ROW(
      FIELD("A", ROW(FIELD("a", STRING.notNull)).nullable),
      FIELD("B", ROW(FIELD("a", STRING.notNull), FIELD("b", INT.notNull)).nullable)
    ).notNull
  }

  it should "derive mixed enum types" in {
    enum Enum {
      case A(a: String)
      case C
    }

    val dataTypeEn = FlinkDataType[Enum]

    // a case without parameters becomes a STRING field of the union (Magnolia lists it first)
    dataTypeEn shouldBe ROW(
      FIELD("C", STRING.nullable),
      FIELD("A", ROW(FIELD("a", STRING.notNull)).nullable)
    ).notNull
  }

  it should "derive empty case classes to Strings" in {
    case class Test()
    val dataType = FlinkDataType[Test]

    // empty case classes are treated like case objects, so that simple enums convert to String
    dataType shouldBe STRING.notNull
  }

  it should "derive objects" in {
    case object Test
    val dataType = FlinkDataType[Test.type]

    dataType shouldBe STRING.notNull
  }

  it should "derive byte iterables" in {
    case class ByteIterables(listBytes: List[Byte], seqBytes: Seq[Byte], vecBytes: Vector[Byte])
    val dataType = FlinkDataType[ByteIterables]

    // List/Seq/Vector[Byte] map to BYTES like Array[Byte], not to ARRAY<INT>
    dataType shouldBe DataTypes.ROW(
      DataTypes.FIELD("listBytes", DataTypes.BYTES.notNull),
      DataTypes.FIELD("seqBytes", DataTypes.BYTES.notNull),
      DataTypes.FIELD("vecBytes", DataTypes.BYTES.notNull)
    ).notNull
  }

  it should "derive None type" in {
    val dataType = FlinkDataType[None.type]

    dataType shouldBe DataTypes.NULL()
  }

  it should "derive Tuple3" in {
    case class Tup3(t: (String, Int, Boolean))
    val dataType = FlinkDataType[Tup3]

    dataType shouldBe ROW(
      FIELD("t", ROW(FIELD("_1", STRING.notNull), FIELD("_2", INT.notNull), FIELD("_3", BOOLEAN.notNull)).notNull)
    ).notNull
  }

  it should "derive Tuple4" in {
    case class Tup4(t: (String, Int, Boolean, Long))
    val dataType = FlinkDataType[Tup4]

    dataType shouldBe ROW(
      FIELD(
        "t",
        ROW(
          FIELD("_1", STRING.notNull),
          FIELD("_2", INT.notNull),
          FIELD("_3", BOOLEAN.notNull),
          FIELD("_4", BIGINT.notNull)
        ).notNull
      )
    ).notNull
  }

  it should "derive Tuple5" in {
    case class Tup5(t: (String, Int, Boolean, Long, Double))
    val dataType = FlinkDataType[Tup5]

    dataType shouldBe ROW(
      FIELD(
        "t",
        ROW(
          FIELD("_1", STRING.notNull),
          FIELD("_2", INT.notNull),
          FIELD("_3", BOOLEAN.notNull),
          FIELD("_4", BIGINT.notNull),
          FIELD("_5", DOUBLE.notNull)
        ).notNull
      )
    ).notNull
  }

  it should "derive Tuple6" in {
    case class Tup6(t: (String, Int, Boolean, Long, Double, Float))
    val dataType = FlinkDataType[Tup6]

    dataType shouldBe DataTypes.ROW(
      DataTypes.FIELD("t", DataTypes.ROW(
        DataTypes.FIELD("_1", DataTypes.STRING.notNull),
        DataTypes.FIELD("_2", DataTypes.INT.notNull),
        DataTypes.FIELD("_3", DataTypes.BOOLEAN.notNull),
        DataTypes.FIELD("_4", DataTypes.BIGINT.notNull),
        DataTypes.FIELD("_5", DataTypes.DOUBLE.notNull),
        DataTypes.FIELD("_6", DataTypes.FLOAT.notNull)
      ).notNull)
    ).notNull
  }
