package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.annotations.TableName
import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters.*

sealed trait SealedTrait

// annotated, so the union field and the schema use a name that differs from the class name
@TableName("A_RENAMED") case class A(a: String) extends SealedTrait

case class B(a: String, b: Int, inner: Inner) extends SealedTrait

case class C(anotherSt: AnotherSt, en: Enum) extends SealedTrait
case object D                                extends SealedTrait

case class Inner(i: Int, en: Enum)

enum Enum {
  case One, Two
}

sealed trait AnotherSt

case class ABC(a: String, en: Enum) extends AnotherSt

case class CBA(a: String, b: Int, inner: Inner) extends AnotherSt

case class Record(id: Int, st: SealedTrait)

object Generators {

  // Enum
  val genEnum: Gen[Enum]                = Gen.oneOf(Enum.One, Enum.Two)
  implicit val arbEnum: Arbitrary[Enum] = Arbitrary(genEnum)

  // Inner
  val genInner: Gen[Inner] = for {
    i  <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    en <- genEnum
  } yield Inner(i, en)
  implicit val arbInner: Arbitrary[Inner] = Arbitrary(genInner)

  // AnotherSt subtypes
  val genABC: Gen[ABC] = for {
    a  <- Gen.alphaNumStr
    en <- genEnum
  } yield ABC(a, en)

  val genCBA: Gen[CBA] = for {
    a     <- Gen.alphaNumStr
    b     <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    inner <- genInner
  } yield CBA(a, b, inner)

  val genAnotherSt: Gen[AnotherSt]                = Gen.oneOf(genABC, genCBA)
  implicit val arbAnotherSt: Arbitrary[AnotherSt] = Arbitrary(genAnotherSt)

  // SealedTrait subtypes
  val genA: Gen[A] = Gen.alphaNumStr.map(A(_))

  val genB: Gen[B] = for {
    a     <- Gen.alphaNumStr
    b     <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    inner <- genInner
  } yield B(a, b, inner)

  val genC: Gen[C] = for {
    anotherSt <- genAnotherSt
    en        <- genEnum
  } yield C(anotherSt, en)

  val genSealedTrait: Gen[SealedTrait]                = Gen.oneOf(genA, genB, genC, Gen.const(D))
  implicit val arbSealedTrait: Arbitrary[SealedTrait] = Arbitrary(genSealedTrait)

  // Record
  val genRecord: Gen[Record] = for {
    id <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    st <- genSealedTrait
  } yield Record(id, st)
  implicit val arbRecord: Arbitrary[Record] = Arbitrary(genRecord)
}

class RoundTripTest extends UnitSpec:

  import com.dtrosien.rowdata4s.Generators.arbRecord

  "Decoder and Encoder" should "convert complex ADTs" in {
    val logicalType                      = FlinkDataType[Record].getLogicalType
    val fromRowData: FromRowData[Record] = FromRowData.apply[Record](logicalType)
    val toRowData: ToRowData[Record]     = ToRowData.apply[Record](logicalType)

    forAll(minSuccessful(5000)) { (record: Record) =>
      val rowData = toRowData.to(record)
      val rRecord = fromRowData.from(rowData)
      record shouldBe rRecord
    }
  }

  it should "convert complex ADTs against a table schema with other field order, an unknown variant and binary rows" in {
    // like a schema taken from a catalog: every ROW (records and unions) lists its fields in reverse order and the
    // union of SealedTrait has a variant the Scala type does not know
    val logicalType = tableSchema(FlinkDataType[Record]).getLogicalType.asInstanceOf[RowType]
    val serializer  = new RowDataSerializer(logicalType)

    val fromRowData: FromRowData[Record] = FromRowData.apply[Record](logicalType)
    val toRowData: ToRowData[Record]     = ToRowData.apply[Record](logicalType)

    forAll(minSuccessful(5000)) { (record: Record) =>
      val rowData: RowData = toRowData.to(record)
      // the binary layout is what the job sees after a shuffle, nested rows and null bits included
      val binaryRow = serializer.toBinaryRow(rowData)
      val rRecord   = fromRowData.from(binaryRow)
      record shouldBe rRecord
    }
  }

  /** Reverses the field order of every ROW and adds an unknown variant to the union of [[SealedTrait]].
    */
  private def tableSchema(dataType: DataType): DataType = {
    def fields(dt: DataType): Seq[DataTypes.Field] =
      dt.getLogicalType.asInstanceOf[RowType].getFieldNames.asScala.zip(dt.getChildren.asScala).map { (name, child) =>
        DataTypes.FIELD(name, reverse(child))
      }.toSeq

    def withNullability(rowType: DataType, original: DataType): DataType =
      if original.getLogicalType.isNullable then rowType.nullable else rowType.notNull

    def reverse(dt: DataType): DataType =
      if dt.getLogicalType.isInstanceOf[RowType] then withNullability(DataTypes.ROW(fields(dt).reverse*), dt)
      else dt

    val unknownVariant = DataTypes.FIELD("Unknown", DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT.notNull)).nullable)
    val (idField, stField) = (fields(dataType)(0), fields(dataType)(1))
    val st = withNullability(DataTypes.ROW((unknownVariant +: fields(stField.getDataType).reverse)*), stField.getDataType)
    DataTypes.ROW(DataTypes.FIELD(stField.getName, st), idField).notNull
  }
