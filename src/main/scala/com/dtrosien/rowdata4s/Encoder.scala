package com.dtrosien.rowdata4s

import com.sksamuel.avro4s.typeutils.Annotations
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import org.apache.flink.table.data.*
import org.apache.flink.table.types.logical.LogicalTypeRoot.*
import org.apache.flink.table.types.logical.*

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.*
import java.util.UUID
import scala.annotation.StaticAnnotation
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

/** Converts a case class T to a Flink [[RowData]]
  */
trait ToRowData[T <: Product] extends Serializable {
  def to(t: T): RowData
}

object ToRowData {
  def apply[T <: Product](logicalType: LogicalType)(using encoder: Encoder[T]): ToRowData[T] = new ToRowData[T] {
    def to(t: T): RowData = encoder.encode(logicalType).apply(t) match {
      case rowData: RowData => rowData
      case output =>
        val clazz = output.getClass
        throw new UnsupportedOperationException(
          s"Cannot marshall an instance of $t to RowData (had class $clazz, output was $output)"
        )
    }
  }
}

/** A [[Encoder]] is used to convert a specified Scala type to RowData.
  */
trait Encoder[T] extends Serializable {
  self =>

  def encode(logicalType: LogicalType): T => Any

  /** Returns an [[Encoder[U]] by applying a function that maps a U to an T, before encoding as an T using this encoder.
    */
  final def contramap[U](f: U => T): Encoder[U] = new Encoder[U] {
    override def encode(logicalType: LogicalType): U => Any = { u => self.encode(logicalType).apply(f(u)) }
  }
}

object Encoder
    extends MagnoliaDerivedEncoder
    with PrimitiveEncoders
    with StringEncoders
    with OptionEncoders
    with CollectionEncoders
    with BigDecimalEncoders
    with ByteIterableEncoders
    with TemporalEncoders {

  /** Returns an [Encoder] that encodes using the supplied function.
    */
  def apply[T](f: T => Any): Encoder[T] = new Encoder[T] {
    def encode(logicalType: LogicalType): T => Any = { t => f(t) }
  }

  /** Returns an [Encoder] that encodes by simply returning the input value.
    */
  def identity[T]: Encoder[T] = Encoder[T](t => t)

  def apply[T](using encoder: Encoder[T]): Encoder[T] = encoder
}

// ==============================================
// Magnolia   ===================================
// ==============================================

trait MagnoliaDerivedEncoder extends AutoDerivation[Encoder]:
  override def join[T](ctx: CaseClass[Encoder, T]): Encoder[T] = new RowEncoder(ctx)

  override def split[T](ctx: SealedTrait[Encoder, T]): Encoder[T] =
    throw UnsupportedOperationException(s"Sealed traits not supported: ${ctx.typeInfo.short}")

// ==============================================
// Row and Field   ==============================
// ==============================================

class RowEncoder[T](ctx: magnolia1.CaseClass[Encoder, T]) extends Encoder[T] {

  def encode(logicalType: LogicalType): T => Any = {
    // the order of the encoders comes from the schema
    val encoders: Array[FieldEncoder[T]] = logicalType
      .asInstanceOf[RowType]
      .getFields
      .asScala
      .map { field =>
        val param = findParam(field, ctx)
        if param.isEmpty then throw new Exception(s"Unable to find case class parameter for field ${field.getName}")
        new FieldEncoder(param.get, field.getType.asInstanceOf)
      }
      .toArray
    { t => encodeT(encoders, t) }
  }

  /** Finds the matching param from the case class for the given Flink [[RowType.RowField]].
    */
  private def findParam(
      field: RowType.RowField,
      ctx: magnolia1.CaseClass[Encoder, T]
  ): Option[CaseClass.Param[Encoder, T]] = {
    ctx.params.find { param =>
      val annotations =
        new Annotations(param.annotations)
      val paramName = annotations.name.getOrElse(param.label)
      paramName == field.getName
    }
  }

  private def encodeT(encoders: Array[FieldEncoder[T]], t: T): GenericRowData = {
    // hot code path. Sacrificing functional programming to the gods of performance.
    val length = encoders.length
    val values = new Array[Any](length)
    var i      = 0
    while i < length do {
      values(i) = encoders(i).encode(t)
      i += 1
    }
    GenericRowData.of(values*)
  }
}

class FieldEncoder[T](param: magnolia1.CaseClass.Param[Encoder, T], logicalType: LogicalType) extends Serializable:

  private val encode = param.typeclass.encode(logicalType.asInstanceOf)

  def encode(record: T): Any = {
    val value = param.deref(record)
    encode.apply(value)
  }

// ==============================================
// Primitives   =================================
// ==============================================

trait PrimitiveEncoders {
  given LongEncoder: Encoder[Long]       = Encoder(a => java.lang.Long.valueOf(a))
  given Encoder[Int]                     = IntEncoder
  given ShortEncoder: Encoder[Short]     = Encoder(a => java.lang.Short.valueOf(a))
  given ByteEncoder: Encoder[Byte]       = Encoder(a => java.lang.Byte.valueOf(a))
  given DoubleEncoder: Encoder[Double]   = Encoder(a => java.lang.Double.valueOf(a))
  given FloatEncoder: Encoder[Float]     = Encoder(a => java.lang.Float.valueOf(a))
  given BooleanEncoder: Encoder[Boolean] = Encoder(a => java.lang.Boolean.valueOf(a))
}

object IntEncoder extends Encoder[Int] {
  override def encode(logicalType: LogicalType): Int => Any = { value => java.lang.Integer.valueOf(value) }
}

// ==============================================
// String   =====================================
// ==============================================

trait StringEncoders:
  given Encoder[String]       = StringEncoder
  given Encoder[CharSequence] = StringEncoder.contramap(_.toString())
  given Encoder[UUID]         = UUIDEncoder

object StringEncoder extends Encoder[String]:
  override def encode(logicalType: LogicalType): String => Any = string => StringData.fromString(string)

object UUIDEncoder extends Encoder[UUID]:
  override def encode(logicalType: LogicalType): UUID => Any = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => uuid => StringEncoder.contramap(_.toString()).encode(logicalType)(uuid)
    case _              => throw new UnsupportedOperationException(s"Unsupported type for uuid: $logicalType")
  }

// ==============================================
// Option   =====================================
// ==============================================

class OptionEncoder[T](encoder: Encoder[T]) extends Encoder[Option[T]] {

  override def encode(logicalType: LogicalType): Option[T] => Any = {
    // nullables must be allowed by schema
    require(
      logicalType.isNullable, {
        "Options can only be encoded when type is nullable"
      }
    )

    val elementEncoder = encoder.encode(logicalType)
    { option => option.fold(null)(elementEncoder) }
  }
}

trait OptionEncoders:
  given [T](using encoder: Encoder[T]): Encoder[Option[T]] = OptionEncoder[T](encoder)

// ==============================================
// Collections   ================================
// ==============================================

trait CollectionEncoders:

  private def iterableEncoder[T, C[X] <: Iterable[X]](encoder: Encoder[T]): Encoder[C[T]] = new Encoder[C[T]] {
    override def encode(logicalType: LogicalType): C[T] => Any = {
      require(logicalType.getTypeRoot == ARRAY)
      val elementEncoder = encoder.encode(logicalType.asInstanceOf[ArrayType].getElementType)
      { t =>
        val arr = t.map(elementEncoder.apply).toArray
        GenericArrayData(arr)
      }
    }
  }

  given [T](using encoder: Encoder[T], tag: ClassTag[T]): Encoder[Array[T]] = new Encoder[Array[T]] {
    override def encode(logicalType: LogicalType): Array[T] => Any = {
      require(logicalType.getTypeRoot == ARRAY)
      val elementEncoder = encoder.encode(logicalType.asInstanceOf[ArrayType].getElementType)
      { t =>
        val arr = t.map(elementEncoder.apply)
        GenericArrayData(arr)
      }
    }
  }

  given [T](using encoder: Encoder[T]): Encoder[List[T]]   = iterableEncoder(encoder)
  given [T](using encoder: Encoder[T]): Encoder[Seq[T]]    = iterableEncoder(encoder)
  given [T](using encoder: Encoder[T]): Encoder[Set[T]]    = iterableEncoder(encoder)
  given [T](using encoder: Encoder[T]): Encoder[Vector[T]] = iterableEncoder(encoder)

  given mapEncoder[K, V](using encoderK: Encoder[K], encoderV: Encoder[V]): Encoder[Map[K, V]] =
    new MapEncoder[K, V](encoderK, encoderV)

class MapEncoder[K, V](encoderK: Encoder[K], encoderV: Encoder[V]) extends Encoder[Map[K, V]]:
  override def encode(logicalType: LogicalType): Map[K, V] => Any = {
    val (encodeK, encodeV) = logicalType.getTypeRoot match
      case MULTISET =>
        (encoderK.encode(IntType()), encoderV.encode(logicalType.asInstanceOf[MultisetType].getElementType))
      case MAP =>
        (
          encoderK.encode(logicalType.asInstanceOf[MapType].getKeyType),
          encoderV.encode(logicalType.asInstanceOf[MapType].getValueType)
        )
      case _ => throw new UnsupportedOperationException(s"Unsupported type for Map: $logicalType")

    { value =>
      val map = new java.util.HashMap[Any, Any]
      value.foreach { case (k, v) => map.put(encodeK.apply(k), encodeV.apply(v)) }
      GenericMapData(map)
    }
  }

// ==============================================
// Big Decimal   ================================
// ==============================================

trait BigDecimalEncoders:
  given Encoder[BigDecimal] = new Encoder[BigDecimal]:
    override def encode(logicalType: LogicalType): BigDecimal => Any = { bd =>
      DecimalData.fromBigDecimal(bd.underlying(), bd.precision, bd.scale)
    }

// ==============================================
// Bytes   ======================================
// ==============================================

trait ByteIterableEncoders:
  given Encoder[ByteBuffer]                                = ByteBufferEncoder
  given Encoder[Array[Byte]]                               = ByteArrayEncoder
  private val IterableByteEncoder: Encoder[Iterable[Byte]] = ByteArrayEncoder.contramap(_.toArray)
  given Encoder[List[Byte]]                                = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Vector[Byte]]                              = IterableByteEncoder.contramap(_.toIterable)
  given Encoder[Seq[Byte]]                                 = IterableByteEncoder.contramap(_.toIterable)

object ByteBufferEncoder extends Encoder[ByteBuffer]:
  override def encode(logicalType: LogicalType): ByteBuffer => Any = {
    logicalType.getTypeRoot match {
      case BINARY | VARBINARY =>
        buffer =>
          val bytes = new Array[Byte](buffer.remaining())
          buffer.get(bytes)
          bytes
      case _ =>
        throw new UnsupportedOperationException(
          s"ByteBufferEncoder doesn't support schema type ${logicalType.getTypeRoot}"
        )
    }
  }

object ByteArrayEncoder extends Encoder[Array[Byte]]:
  override def encode(logicalType: LogicalType): Array[Byte] => Any = {
    logicalType.getTypeRoot match {
      case BINARY | VARBINARY => identity
      case _ =>
        throw new UnsupportedOperationException(
          s"ByteArrayEncoder doesn't support schema type ${logicalType.getTypeRoot}"
        )
    }
  }

// ==============================================
// Temporal   ===================================
// ==============================================

trait TemporalEncoders:
  given Encoder[Instant]                     = InstantEncoder
  given TimestampEncoder: Encoder[Timestamp] = InstantEncoder.contramap[Timestamp](_.toInstant)

  given LocalDateEncoder: Encoder[LocalDate] = IntEncoder.contramap[LocalDate](_.toEpochDay.toInt)
  given Encoder[LocalTime]                   = LocalTimeEncoder
  given Encoder[LocalDateTime]               = LocalDateTimeEncoder

  given DateEncoder: Encoder[Date] = IntEncoder.contramap[Date](_.toLocalDate.toEpochDay.toInt)
  given OffsetDateTimeEncoder: Encoder[OffsetDateTime] =
    StringEncoder.contramap[OffsetDateTime](_.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))

// flink uses TIME(9)
object LocalTimeEncoder extends Encoder[LocalTime]:
  override def encode(logicalType: LogicalType): LocalTime => Any = {
    { value => java.lang.Long.valueOf(value.toNanoOfDay) }
  }

object InstantEncoder extends Encoder[Instant]:
  override def encode(logicalType: LogicalType): Instant => Any = {
    { value => TimestampData.fromInstant(value) }
  }

object LocalDateTimeEncoder extends Encoder[LocalDateTime]:
  private def epochMillis(temporal: LocalDateTime): Long  = temporal.toInstant(ZoneOffset.UTC).toEpochMilli
  private def epochSeconds(temporal: LocalDateTime): Long = temporal.toEpochSecond(ZoneOffset.UTC)
  private def nanos(temporal: LocalDateTime): Long        = temporal.getNano.toLong

  override def encode(logicalType: LogicalType): LocalDateTime => Any = {
    logicalType.getTypeRoot match
      case BIGINT => value => java.lang.Long.valueOf(epochMillis(value))
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE =>
        value => TimestampData.fromLocalDateTime(value)
      case _ =>
        throw new UnsupportedOperationException(
          s"LocalDateTimeEncoder doesn't support schema type ${logicalType.getTypeRoot}"
        )
  }
