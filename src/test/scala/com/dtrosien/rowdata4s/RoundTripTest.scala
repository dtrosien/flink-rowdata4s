package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.datatype.FlinkDataType
import org.scalacheck.{Arbitrary, Gen}

sealed trait SealedTrait

case class A(a: String) extends SealedTrait

case class B(a: String, b: Int, inner: Inner) extends SealedTrait

case class C(anotherSt: AnotherSt, en: Enum) extends SealedTrait

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

  val genSealedTrait: Gen[SealedTrait]                = Gen.oneOf(genA, genB, genC)
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
