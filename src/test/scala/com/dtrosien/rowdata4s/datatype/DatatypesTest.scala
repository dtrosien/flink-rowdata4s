package com.dtrosien.rowdata4s.datatype

import com.dtrosien.rowdata4s.UnitSpec
import com.dtrosien.rowdata4s.annotations.{TableName, TableTransient}
import com.sksamuel.avro4s.{AvroName, AvroSchema, AvroTransient}
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter

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
    val schema = AvroSchema[Strings]
    val dataTypeFromSchema = AvroSchemaConverter.convertToDataType(schema.toString)
    val dataType = FlinkDataType[Strings]

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
