package com.dtrosien.rowdata4s.datatype

import org.apache.flink.table.types.DataType

class FlinkDataType {}

/** Creates a Flink DataType for an arbitrary type T.
  *
  * This type is called [[FlinkDataType]] and not just [[DataType]] to facilitate easy importing when mixing with
  * [[org.apache.flink.table.types.DataType]].
  */
object FlinkDataType {

  /** Generates an [[org.apache.flink.table.types.DataType]] for a type T using default configuration.
    *
    * Requires an instance of [[DataTypeFor]]
    */
  def apply[T](using dataTypeFor: DataTypeFor[T]): DataType = dataTypeFor.dataType
}
