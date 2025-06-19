package com.dtrosien.rowdata4s

import com.sksamuel.avro4s.typeutils.Annotations
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import org.apache.flink.table.data.RowData.FieldGetter
import org.apache.flink.table.data.*
import org.apache.flink.table.types.logical.LogicalTypeRoot.*
import org.apache.flink.table.types.logical.*

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.*
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** Converts from a Flink [[RowData]] into instances of T.
  */
trait FromRowData[T] extends Serializable {
  def from(rowData: RowData): T
}

object FromRowData {
  def apply[T](logicalType: LogicalType)(using decoder: Decoder[T]): FromRowData[T] = new FromRowData[T] {
    override def from(rowData: RowData): T = decoder.decode(logicalType).apply(rowData)
  }
}

/** A [[Decoder]] is used to convert RowData into a specified Scala type.
  */
trait Decoder[T] extends Serializable {
  self =>

  def decode(logicalType: LogicalType): Any => T

  final def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(logicalType: LogicalType): Any => U = { input =>
      f(self.decode(logicalType).apply(input))
    }
  }
}

object Decoder
    extends MagnoliaDerivedDecoder
    with PrimitiveDecoders
    with StringDecoders
    with OptionDecoders
    with CollectionDecoders
    with BigDecimalDecoders
    with ByteDecoders
    with TemporalDecoders {
  def apply[T](using decoder: Decoder[T]): Decoder[T] = decoder
}

// ==============================================
// Magnolia   ===================================
// ==============================================

trait MagnoliaDerivedDecoder extends AutoDerivation[Decoder]:

  override def join[T](ctx: CaseClass[Decoder, T]): Decoder[T] =
    RowDecoder(ctx)

  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] =
    throw UnsupportedOperationException(s"Sealed traits not supported: ${ctx.typeInfo.short}")

// ==============================================
// Row and Field   ==============================
// ==============================================

class RowDecoder[T](ctx: magnolia1.CaseClass[Decoder, T]) extends Decoder[T] {

  override def decode(logicalType: LogicalType): Any => T = {
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala

    val decoders = fields.zipWithIndex.map { case (field, i) =>
      val param       = findParam(field, ctx)
      val fieldType   = field.getType
      val fieldGetter = RowData.createFieldGetter(fieldType, i)
      if param.isEmpty then throw new Exception(s"Unable to find case class parameter for field ${field.getName}")
      new FieldDecoder(param.get, fieldType, fieldGetter)
    }.toArray

    t => decodeT(logicalType, decoders, t)
  }

  /** Finds the matching param from the case class for the given Flink [[RowType.RowField]].
    */
  private def findParam(
      field: RowType.RowField,
      ctx: magnolia1.CaseClass[Decoder, T]
  ): Option[CaseClass.Param[Decoder, T]] = {
    ctx.params.find { param =>
      val annotations =
        new Annotations(param.annotations)
      val paramName = annotations.name.getOrElse(param.label)
      paramName == field.getName
    }
  }

  private def decodeT(logicalType: LogicalType, decoders: Array[FieldDecoder[T]], value: Any): T = value match {
    case rowData: RowData =>
      // hot code path. Sacrificing functional programming to the gods of performance.
      val length = decoders.length
      val values = new Array[Any](length)
      var i      = 0
      while i < length do {
        values(i) = decoders(i).decode(rowData)
        i += 1
      }
      ctx.rawConstruct(values.toIndexedSeq)
    case _ =>
      throw new UnsupportedOperationException(s"This decoder can only handle RowData [was ${value.getClass}]")
  }
}

/** Decodes normal fields based on the schema.
  */
class FieldDecoder[T](
    param: magnolia1.CaseClass.Param[Decoder, T],
    logicalType: LogicalType,
    fieldGetter: RowData.FieldGetter
) {
  private val decoder = param.typeclass.asInstanceOf[Decoder[T]].decode(logicalType)

  def decode(rowData: RowData): Any = {
    fastDecodeFieldValue(rowData, fieldGetter)
  }

  private def fastDecodeFieldValue(rowData: RowData, fieldGetter: FieldGetter): Any =
    if fieldGetter == null then defaultFieldValue
    else tryDecode(fieldGetter.getFieldOrNull(rowData))

  @inline
  private def defaultFieldValue: Any = param.default match {
    case Some(default) => default
    // there is no default, so the field must be an option
    case None => decoder.apply(null)
  }

  @inline
  private def tryDecode(value: Any): Any =
    try {
      decoder.apply(value)
    } catch {
      case NonFatal(ex) => param.default.getOrElse(throw ex)
    }
}

// ==============================================
// Primitives   =================================
// ==============================================

trait PrimitiveDecoders {

  given Decoder[Byte] = new BasicDecoder[Byte] {
    override def decode(value: Any): Byte = value match {
      case b: Byte => b
      case _       => value.asInstanceOf[Int].byteValue
    }
  }

  given Decoder[Short] = new BasicDecoder[Short] {
    override def decode(value: Any): Short = value match {
      case b: Byte  => b
      case s: Short => s
      case i: Int   => i.toShort
    }
  }

  given IntDecoder: Decoder[Int] = new BasicDecoder[Int] {
    override def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => throw new UnsupportedOperationException(s"Cannot convert $other to type INT")
    }
  }

  given Decoder[Long] = new BasicDecoder[Long] {
    override def decode(value: Any): Long = value match {
      case byte: Byte   => byte.toLong
      case short: Short => short.toLong
      case int: Int     => int.toLong
      case long: Long   => long
      case other        => throw new UnsupportedOperationException(s"Cannot convert $other to type LONG")
    }
  }

  given Decoder[Double] = new BasicDecoder[Double] {
    override def decode(value: Any): Double = value match {
      case d: Double           => d
      case d: java.lang.Double => d
    }
  }

  given Decoder[Float] = new BasicDecoder[Float] {
    override def decode(value: Any): Float = value match {
      case f: Float           => f
      case f: java.lang.Float => f
    }
  }

  given Decoder[Boolean] = new BasicDecoder[Boolean] {
    override def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }
}

/** A [[BasicDecoder]] is one that does not require the [[Schema]].
  */
trait BasicDecoder[T] extends Decoder[T]:
  def decode(value: Any): T

  override def decode(logicalType: LogicalType): Any => T = { value => decode(value) }

// ==============================================
// String   =====================================
// ==============================================

trait StringDecoders:
  given Decoder[String]       = StringDecoder
  given Decoder[CharSequence] = CharSequenceDecoder
  given Decoder[UUID]         = StringDecoder.map(UUID.fromString)

/** A [[Decoder]] for Strings that pattern matches on the incoming type to decode.
  *
  * The schema is not used, meaning this decoder is forgiving of types that do not conform to the schema, but are
  * nevertheless usable.
  */
object StringDecoder extends Decoder[String]:
  override def decode(logicalType: LogicalType): Any => String = {
    case stringData: StringData => stringData.toString
    case string: String         => string
    case charseq: CharSequence  => charseq.toString
    case other: Any =>
      throw new UnsupportedOperationException(s"Unsupported type $other ${other.getClass} for StringDecoder")
  }

object CharSequenceDecoder extends Decoder[CharSequence]:
  override def decode(logicalType: LogicalType): Any => CharSequence = {
    case stringData: StringData => stringData.toString
    case string: String         => string
    case charseq: CharSequence  => charseq
    case other: Any =>
      throw new UnsupportedOperationException(s"Unsupported type $other ${other.getClass} for StringDecoder")
  }

object StrictStringDecoder extends Decoder[String]:
  override def decode(logicalType: LogicalType): Any => String = logicalType.getTypeRoot match {
    case VARCHAR | CHAR => StringDataDecoder.decode(logicalType)
    case _              => throw new UnsupportedOperationException(s"Unsupported type for string schema: $logicalType")
  }

object StringDataDecoder extends Decoder[String]:
  override def decode(logicalType: LogicalType): Any => String = { case stringData: StringData =>
    stringData.toString
  }

// ==============================================
// Option   =====================================
// ==============================================

class OptionDecoder[T](decoder: Decoder[T]) extends Decoder[Option[T]] {

  override def decode(logicalType: LogicalType): Any => Option[T] = {
    // nullables must be allowed by schema
    require(
      logicalType.isNullable, {
        "Options can only be decoded when type is nullable"
      }
    )

    val decode = decoder.decode(logicalType)
    { value => if value == null then None else Some(decode(value)) }
  }
}

trait OptionDecoders:
  given [T](using decoder: Decoder[T]): Decoder[Option[T]] = new OptionDecoder[T](decoder)

// ==============================================
// Collections   ================================
// ==============================================

class ArrayDecoder[T: ClassTag](decoder: Decoder[T]) extends Decoder[Array[T]]:
  def decode(logicalType: LogicalType): Any => Array[T] = {
    require(
      logicalType.getTypeRoot == ARRAY, {
        s"Require logicalType ARRAY (was $logicalType)"
      }
    )
    val elementType = logicalType.asInstanceOf[ArrayType].getElementType
    val decodeT     = decoder.decode(elementType)
    {
      case arrayData: ArrayData =>
        val elementGetter = ArrayData.createElementGetter(elementType)
        (0 until arrayData.size()).map(i => decodeT(elementGetter.getElementOrNull(arrayData, i))).toArray
      case array: Array[?]               => array.map(decodeT)
      case list: java.util.Collection[?] => list.asScala.map(decodeT).toArray
      case list: Iterable[?]             => list.map(decodeT).toArray
      case other                         => throw new UnsupportedOperationException("Unsupported array " + other)
    }
  }

trait CollectionDecoders:
  given [T: ClassTag](using decoder: Decoder[T]): Decoder[Array[T]] = ArrayDecoder[T](decoder)
  given [T](using decoder: Decoder[T]): Decoder[List[T]]            = iterableDecoder(decoder, _.toList)
  given [T](using decoder: Decoder[T]): Decoder[Seq[T]]             = iterableDecoder(decoder, _.toSeq)
  given [T](using decoder: Decoder[T]): Decoder[Set[T]]             = iterableDecoder(decoder, _.toSet)
  given [T](using decoder: Decoder[T]): Decoder[Vector[T]]          = iterableDecoder(decoder, _.toVector)
  given [K, T](using decoderK: Decoder[K], decoderV: Decoder[T]): Decoder[Map[K, T]] =
    new MapDecoder[K, T](decoderK, decoderV)

  private def iterableDecoder[T, C[X] <: Iterable[X]](decoder: Decoder[T], build: Iterable[T] => C[T]): Decoder[C[T]] =
    new Decoder[C[T]] {
      def decode(logicalType: LogicalType): Any => C[T] = {
        require(
          logicalType.getTypeRoot == ARRAY, {
            s"Require logicalType ARRAY (was $logicalType)"
          }
        )
        val elementType = logicalType.asInstanceOf[ArrayType].getElementType
        val decodeT     = decoder.decode(elementType)
        {
          case arrayData: ArrayData =>
            val elementGetter = ArrayData.createElementGetter(elementType)
            build((0 until arrayData.size()).map(i => decodeT(elementGetter.getElementOrNull(arrayData, i))))
          case list: java.util.Collection[?] => build(list.asScala.map(decodeT))
          case list: Iterable[?]             => build(list.map(decodeT))
          case array: Array[?]               =>
            // converting array to Seq in order to avoid requiring ClassTag[T] as does arrayDecoder.
            build(array.toSeq.map(decodeT))
          case other => throw new UnsupportedOperationException("Unsupported collection type " + other)
        }
      }
    }

class MapDecoder[K, V](decoderK: Decoder[K], decoderV: Decoder[V]) extends Decoder[Map[K, V]]:
  override def decode(logicalType: LogicalType): Any => Map[K, V] = {
    val (keyType, valueType, decodeK, decodeV) = logicalType.getTypeRoot match
      case MULTISET =>
        val valueType = logicalType.asInstanceOf[MultisetType].getElementType
        (IntType(), valueType, decoderK.decode(IntType()), decoderV.decode(valueType))
      case MAP =>
        val keyType   = logicalType.asInstanceOf[MapType].getKeyType
        val valueType = logicalType.asInstanceOf[MapType].getValueType
        (keyType, valueType, decoderK.decode(keyType), decoderV.decode(valueType))
      case _ => throw new UnsupportedOperationException(s"Unsupported type for Map: $logicalType")

    {
      case mapData: MapData =>
        val keyGetter   = ArrayData.createElementGetter(keyType)
        val valueGetter = ArrayData.createElementGetter(valueType)
        (0 until mapData.size()).map { i =>
          val k = decodeK(keyGetter.getElementOrNull(mapData.keyArray(), i))
          val v = decodeV(valueGetter.getElementOrNull(mapData.valueArray(), i))
          k -> v
        }.toMap

      case map: java.util.Map[?, ?] =>
        map.asScala.toMap.map { case (k, v) => decodeK(k) -> decodeV(v) }
    }
  }

// ==============================================
// Big Decimal   ================================
// ==============================================

trait BigDecimalDecoders:
  given Decoder[BigDecimal] = new Decoder[BigDecimal]:
    override def decode(logicalType: LogicalType): Any => BigDecimal = {
      logicalType.getTypeRoot match {
        case DECIMAL => DecimalDataDecoder.decode(logicalType)
        case VARCHAR => BigDecimalStringDecoder.decode(logicalType)
        case t =>
          throw new UnsupportedOperationException(
            s"Unable to create Decoder with schema type $t, only decimal is supported"
          )
      }
    }

object DecimalDataDecoder extends Decoder[BigDecimal] {
  override def decode(logicalType: LogicalType): Any => BigDecimal = { case decimalData: DecimalData =>
    decimalData.toBigDecimal
  }
}

object BigDecimalStringDecoder extends Decoder[BigDecimal] {
  override def decode(logicalType: LogicalType): Any => BigDecimal = {
    val decode = Decoder[String].decode(logicalType)
    { value => BigDecimal(decode(value)) }
  }
}

// ==============================================
// Bytes   ======================================
// ==============================================

trait ByteDecoders:
  given Decoder[Array[Byte]]  = ArrayByteDecoder
  given Decoder[ByteBuffer]   = ByteBufferDecoder
  given Decoder[List[Byte]]   = ArrayByteDecoder.map(_.toList)
  given Decoder[Seq[Byte]]    = ArrayByteDecoder.map(_.toList)
  given Decoder[Vector[Byte]] = ArrayByteDecoder.map(_.toVector)

/** A [[Decoder]] for byte arrays that accepts any compatible type regardless of schema.
  */
object ArrayByteDecoder extends Decoder[Array[Byte]]:
  override def decode(logicalType: LogicalType): Any => Array[Byte] = {
    case buffer: ByteBuffer =>
      val bytes = new Array[Byte](buffer.remaining())
      buffer.get(bytes)
      bytes
    case array: Array[Byte] => array
    case other              => throw new UnsupportedOperationException(s"ArrayByteDecoder cannot decode '$other'")
  }

object ByteBufferDecoder extends Decoder[ByteBuffer]:
  override def decode(logicalType: LogicalType): Any => ByteBuffer = {
    case buffer: ByteBuffer => buffer
    case array: Array[Byte] => ByteBuffer.wrap(array)
    case other              => throw new UnsupportedOperationException(s"ByteBufferDecoder cannot decode '$other'")
  }

// ==============================================
// Temporal   ===================================
// ==============================================

trait TemporalDecoders:
  given TimestampDecoder: Decoder[Timestamp]         = InstantDecoder.map[Timestamp](Timestamp.from)
  given DateDecoder: Decoder[Date]                   = LocalDateDecoder.map[Date](Date.valueOf)
  given LocalDateTimeDecoder: Decoder[LocalDateTime] = InstantDecoder.map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
  given LocalTimeDecoder: Decoder[LocalTime]         = InstantDecoder.map(LocalTime.ofInstant(_, ZoneOffset.UTC))
  given LocalDateDecoder: Decoder[LocalDate] = Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong))

  given OffsetDateTimeDecoder: Decoder[OffsetDateTime] =
    StringDecoder.map(OffsetDateTime.parse(_, DateTimeFormatter.ISO_OFFSET_DATE_TIME))

  given InstantDecoder: Decoder[Instant] = new Decoder[Instant] {
    override def decode(logicalType: LogicalType): Any => Instant = {
      case timestampData: TimestampData => timestampData.toInstant
      case l: Long                      => Instant.ofEpochMilli(l)
      case i: Int                       => Instant.ofEpochMilli(i.toLong)
      case other => throw new IllegalArgumentException(s"Unsupported type for Instant decoding: ${other.getClass}")
    }
  }
