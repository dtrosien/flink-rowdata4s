package com.dtrosien.rowdata4s.datatype

import com.dtrosien.rowdata4s.UnitSpec
import com.dtrosien.rowdata4s.annotations.{TableName, TableTransient}
import com.sksamuel.avro4s.{AvroName, AvroSchema, AvroTransient}
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.DataTypes

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.util.UUID

class DatatypesTest extends UnitSpec:

  "DatatypesFor" should "derive Primitives" in {
    case class Primitives(int: Int, long: Long, double: Double, float: Float, boolean: Boolean)
    val schema             = AvroSchema[Primitives]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Primitives]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive Optionals" in {
    case class Optionals(maybeInt: Option[Int], maybeString: Option[String])
    val schema             = AvroSchema[Optionals]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Optionals]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive Strings" in {
    case class Strings(uuid: UUID, str: String, charSequence: CharSequence)
    val schema             = AvroSchema[Strings]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Strings]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive nested data" in {
    case class Test(id: Int, num: Long)
    case class TestNested(id: Int, num: Long, inner: Test)
    val schema             = AvroSchema[TestNested]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[TestNested]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive Collections" in {
    case class Inner(name: String)
    case class Collections(
        list: List[Boolean],
        map: Map[String, Inner],
        arr: Array[Option[Inner]],
        seq: Seq[Int],
        vec: Vector[String]
    )
    val schema             = AvroSchema[Collections]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Collections]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive BigDecimal" in {
    case class Decimal(dec: BigDecimal)
    val schema             = AvroSchema[Decimal]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Decimal]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive Bytes" in {
    case class Bytes(bytesArray: Array[Byte], bytebuffer: ByteBuffer)
    val schema             = AvroSchema[Bytes]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Bytes]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive Tuples" in {
    case class Tup(tuple: (String, String))
    val schema             = AvroSchema[Tup]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Tup]

    dataType shouldBe dataTypeFromSchema
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
    val schema             = AvroSchema[TimeAndDates]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[TimeAndDates]

    dataType shouldBe dataTypeFromSchema
  }

  it should "use Annotations" in {
    case class Test(@TableName("ID_RENAMED") @AvroName("ID_RENAMED") id: Int, @TableTransient @AvroTransient id2: Int)
    val schema             = AvroSchema[Test]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Test]

    dataType shouldBe dataTypeFromSchema
  }

  it should "derive enum types" in {
    sealed trait SealedTrait
    case object Test1 extends SealedTrait
    case object Test2 extends SealedTrait

    enum Enum {
      case A, B
    }

    val dataTypeSt           = FlinkDataType[SealedTrait]
    val dataTypeEn           = FlinkDataType[Enum]
    val schemaSt             = AvroSchema[SealedTrait]
    val schemaEn             = AvroSchema[Enum]
    val dataTypeFromSchemaSt = AvroSchemaConverter.convertToDataType(schemaSt.toString)
    val dataTypeFromSchemaEn = AvroSchemaConverter.convertToDataType(schemaEn.toString)

    // enums are encoded inside a row, which differs from Avro encoding
    dataTypeSt shouldBe dataTypeFromSchemaSt
    dataTypeEn shouldBe dataTypeFromSchemaEn
    // println(schemaEn)
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

    // println(dataType.getLogicalType)
  }

  it should "derive rich enum types" in {
    enum Enum {
      case A(a: String)
      case B(a: String, b: Int)
    }

    val dataTypeEn = FlinkDataType[Enum]
    val schemaEn   = AvroSchema[Enum]

    // println(schemaEn)
  }

  it should "derive empty case classes to Strings" in {
    case class Test()
    val schema             = AvroSchema[Test]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Test]

    // deviates from Avro to make simple Enums directly convert to String!!!
    // dataType shouldBe dataTypeFromSchema
    dataType shouldBe DataTypes.STRING.notNull
  }

  it should "derive objects" in {
    case object Test
    val schema             = AvroSchema[Test.type]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType           = FlinkDataType[Test.type]

    // deviates from Avro to make objects directly convert to String!!!
    // dataType shouldBe dataTypeFromSchema
    dataType shouldBe DataTypes.STRING.notNull
  }
