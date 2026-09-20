package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.annotations.{Annotations, Names}
import com.dtrosien.rowdata4s.datatype.{CaseClassShape, DatatypeShape, SealedTraitShape}
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import org.apache.flink.table.data.*
import org.apache.flink.table.types.logical.*
import org.apache.flink.table.types.logical.LogicalTypeRoot.*

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.NotGiven
import scala.util.control.NonFatal


/** Converts from a Flink [[RowData]] into instances of T.
  */
trait FromRowData[T <: Product] extends Serializable {
  def from(rowData: RowData): T
}

object FromRowData {
  def apply[T <: Product](
      logicalType: LogicalType
  )(using decoder: Decoder[T], notEnum: NotGiven[T <:< scala.reflect.Enum]): FromRowData[T] = {
    // cache resolved schema
    val decode: Any => T = decoder.decode(logicalType)
    new FromRowData[T] {
      override def from(rowData: RowData): T = decode(rowData)
    }
  }
}

/** A [[Decoder]] is used to convert RowData into a specified Scala type.
  */
trait Decoder[T] extends Serializable {
  self =>

  def decode(logicalType: LogicalType): Any => T

  final def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(logicalType: LogicalType): Any => U = {
      val decodeT = self.decode(logicalType)
      input => f(decodeT(input))
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
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record    => RowDecoder(ctx)
      case CaseClassShape.ValueType => RowDecoder(ctx)
      case CaseClassShape.Object    => ObjectDecoder(ctx)
    }

  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] =
    DatatypeShape.of[T](ctx) match {
      case SealedTraitShape.TypeUnion =>
        ctx.subtypes match {
          case IArray(single) => single.typeclass.asInstanceOf[Decoder[T]]
          case multiple       => TypeUnionDecoder(ctx)
        }
      case SealedTraitShape.Enum => EnumDecoder(ctx)
    }

// ==============================================
// TypeUnions   =================================
// ==============================================

class TypeUnionDecoder[T](ctx: magnolia1.SealedTrait[Decoder, T]) extends Decoder[T] {
  override def decode(logicalType: LogicalType): Any => T = {
    require(logicalType.getTypeRoot == LogicalTypeRoot.ROW)
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala

    val namedSubtypes: Seq[(String, SealedTrait.Subtype[Decoder, T, ?])] =
      ctx.subtypes.map(st => Names(st.typeInfo, new Annotations(st.annotations, st.inheritedAnnotations)).name -> st)

    { value =>
      val row = value.asInstanceOf[RowData]

      val (activeField, activeIndex) = fields.zipWithIndex
        .find { case (_, i) => !row.isNullAt(i) }
        .getOrElse(throw new RuntimeException("All fields are null in union ROW"))

      val (_, st) = namedSubtypes
        .find { case (name, _) => name == activeField.getName }
        .getOrElse(throw new RuntimeException(s"No subtype found for field ${activeField.getName}"))

      val fieldValue = RowData.createFieldGetter(activeField.getType, activeIndex).getFieldOrNull(row)
      st.typeclass.asInstanceOf[Decoder[T]].decode(activeField.getType)(fieldValue)
    }
  }
}

// ==============================================
// Enums   ===================================
// ==============================================

class EnumDecoder[T](ctx: magnolia1.SealedTrait[Decoder, T]) extends Decoder[T] {
  override def decode(logicalType: LogicalType): Any => T = {
    require(logicalType.getTypeRoot == LogicalTypeRoot.VARCHAR)

    val decodeString = StringDecoder.decode(logicalType)

    val decodersByName: Map[String, Any => T] = ctx.subtypes.map { st =>
      Names(st.typeInfo, new Annotations(st.annotations, st.inheritedAnnotations)).name -> st.typeclass.decode(logicalType)
    }.toMap

    { value =>
      val strValue = decodeString(value)
      decodersByName.get(strValue) match {
        case Some(decodeSubtype) => decodeSubtype(value)
        case None =>
          throw new IllegalArgumentException(
            s"Unknown value '$strValue' for ${ctx.typeInfo.full}, expected one of: ${decodersByName.keys.mkString(", ")}"
          )
      }
    }
  }
}

// ==============================================
// Objects   ===================================
// ==============================================

class ObjectDecoder[T](ctx: magnolia1.CaseClass[Decoder, T]) extends Decoder[T] {
  override def decode(logicalType: LogicalType): Any => T = { value => ctx.rawConstruct(Nil) }
}

// ==============================================
// Row and Field   ==============================
// ==============================================

/** Decodes a ROW into a case class. Columns are matched to case class parameters by name, so the column order in the
  * schema does not have to match the parameter order. Columns without a parameter are ignored; a parameter without a
  * column takes its default value, or None if it is an Option.
  */
class RowDecoder[T](ctx: magnolia1.CaseClass[Decoder, T]) extends Decoder[T] {

  override def decode(logicalType: LogicalType): Any => T = {
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala

    // one decoder per case class parameter, in parameter order, so the values match the constructor
    val decoders = ctx.params.toList.map { param =>
      val paramName = new Annotations(param.annotations).name.getOrElse(param.label)
      fields.indexWhere(_.getName == paramName) match {
        case -1 => FieldDecoder.missing(param)
        case i  => FieldDecoder(param, fields(i).getType, RowData.createFieldGetter(fields(i).getType, i))
      }
    }.toArray

    t => decodeT(logicalType, decoders, t)
  }

  private def decodeT(logicalType: LogicalType, decoders: Array[FieldDecoder[T]], value: Any): T = value match {
    case rowData: RowData =>
      val length = decoders.length
      val values = new Array[Any](length)
      var i      = 0
      while i < length do {
        values(i) = decoders(i).decode(rowData)
        i += 1
      }
      ctx.rawConstruct(values)
    case _ =>
      throw new UnsupportedOperationException(s"This decoder can only handle RowData [was ${value.getClass}]")
  }
}

/** Decodes one case class parameter from a row.
  */
sealed abstract class FieldDecoder[T] extends Serializable {
  def decode(rowData: RowData): Any
}

object FieldDecoder {

  /** Decodes normal fields based on the schema.
    */
  def apply[T](
      param: magnolia1.CaseClass.Param[Decoder, T],
      logicalType: LogicalType,
      fieldGetter: RowData.FieldGetter
  ): FieldDecoder[T] = new FieldDecoder[T] {
    private val decoder = param.typeclass.asInstanceOf[Decoder[T]].decode(logicalType)

    def decode(rowData: RowData): Any = tryDecode(fieldGetter.getFieldOrNull(rowData))

    @inline
    private def tryDecode(value: Any): Any =
      try {
        decoder.apply(value)
      } catch {
        case NonFatal(ex) =>
          param.default.getOrElse(
            throw new RowDataDecodingException(
              s"Cannot decode field '${param.label}' from column of type $logicalType",
              ex
            )
          )
      }
  }

  /** Decoder for a case class parameter without a column in the schema: the default value, None for an Option, or a
    * failure while building the decoder so that a schema mismatch is found before the first record.
    */
  def missing[T](param: magnolia1.CaseClass.Param[Decoder, T]): FieldDecoder[T] = param.default match {
    case Some(default)                                          => constant(default)
    case None if param.typeclass.isInstanceOf[OptionDecoder[?]] => constant(None)
    case None =>
      throw new IllegalArgumentException(
        s"Schema has no column for parameter '${param.label}' and the parameter has no default value"
      )
  }

  private def constant[T](value: Any): FieldDecoder[T] = new FieldDecoder[T] {
    def decode(rowData: RowData): Any = value
  }
}

// ==============================================
// Primitives   =================================
// ==============================================

trait PrimitiveDecoders {

  given Decoder[Byte] = new BasicDecoder[Byte] {
    override def decode(value: Any): Byte = value match {
      case byte: Byte   => byte
      case short: Short => short.toByte
      case int: Int     => int.toByte
      case other        => throw new UnsupportedOperationException(s"Cannot convert $other to type BYTE")
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
    override def decode(value: Any): Boolean = value match {
      case boolean: Boolean => boolean
      case other            => throw new UnsupportedOperationException(s"Cannot convert $other to type BOOLEAN")
    }
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
    val elementGetter = ArrayData.createElementGetter(elementType)
    {
      case arrayData: ArrayData =>
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
        val elementGetter = ArrayData.createElementGetter(elementType)
        {
          case arrayData: ArrayData =>
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

    val keyGetter   = ArrayData.createElementGetter(keyType)
    val valueGetter = ArrayData.createElementGetter(valueType)
    {
      case mapData: MapData =>
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
  given LocalTimeDecoder: Decoder[LocalTime] = new Decoder[LocalTime] {
    override def decode(logicalType: LogicalType): Any => LocalTime = {
      // Flink stores TIME_WITHOUT_TIME_ZONE as milliseconds-of-day as an int internally
      case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1_000_000)
      case l: Long => LocalTime.ofNanoOfDay(l * 1_000_000)
    }
  }
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


// ==============================================
// Utils   ===================================
// ==============================================

/** Thrown when a field of a [[RowData]] cannot be decoded; names the field and the column type, the cause is the
  * underlying error.
  */
class RowDataDecodingException(message: String, cause: Throwable) extends RuntimeException(message, cause)