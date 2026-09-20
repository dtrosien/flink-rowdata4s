package com.dtrosien.rowdata4s

import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.logical.RowType

case class Deci(dec: BigDecimal)

class BigDecimalEncoderTest extends UnitSpec:

  private val rowType: RowType =
    DataTypes.ROW(DataTypes.FIELD("dec", DataTypes.DECIMAL(10, 2).notNull)).notNull.getLogicalType.asInstanceOf[RowType]

  private def serializerRoundTrip(rowData: RowData): RowData =
    val ser = new RowDataSerializer(rowType)
    val out = new DataOutputSerializer(64)
    ser.serialize(rowData, out)
    ser.deserialize(new DataInputDeserializer(out.getCopyOfBuffer))

  "BigDecimalEncoder" should "encode with the column's precision and scale so the value survives serialization" in {
    val encoded = ToRowData[Deci](rowType).to(Deci(BigDecimal("1.5")))
    encoded.getDecimal(0, 10, 2).scale shouldBe 2
    serializerRoundTrip(encoded).getDecimal(0, 10, 2).toBigDecimal shouldBe new java.math.BigDecimal("1.50")
    FromRowData[Deci](rowType).from(serializerRoundTrip(encoded)) shouldBe Deci(BigDecimal("1.50"))
  }

  it should "round extra fractional digits HALF_UP" in {
    ToRowData[Deci](rowType).to(Deci(BigDecimal("1.005"))).getDecimal(0, 10, 2).toString shouldBe "1.01"
  }

  it should "reject values whose integer part does not fit the column" in {
    an[IllegalArgumentException] should be thrownBy ToRowData[Deci](rowType).to(Deci(BigDecimal("123456789.123")))
  }
