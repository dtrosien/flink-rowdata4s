package com.dtrosien.rowdata4s

import com.dtrosien.rowdata4s.annotations.{Annotations, Names}
import com.dtrosien.rowdata4s.datatype.{CaseClassShape, DatatypeShape, SealedTraitShape}
import magnolia1.SealedTrait.Subtype
import magnolia1.{AutoDerivation, CaseClass, SealedTrait}
import org.apache.flink.table.data.*
import org.apache.flink.table.types.logical.*
import org.apache.flink.table.types.logical.LogicalTypeRoot.*
import org.apache.flink.types.RowKind

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.*
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.NotGiven

/** Converts a case class T to a Flink [[RowData]]
  */
trait ToRowData[T <: Product] extends Serializable {

  /** Converts `t` to a [[RowData]] with row kind `INSERT`.
    */
  def to(t: T): RowData = to(t, RowKind.INSERT)

  /** Converts `t` to a [[RowData]] carrying the given [[RowKind]], e.g. to forward the kind of a changelog record
    * (`toRowData.to(value, sourceRow.getRowKind)`) or to emit a `DELETE` for an upsert sink.
    */
  def to(t: T, rowKind: RowKind): RowData
}

object ToRowData {
  def apply[T <: Product](
      logicalType: LogicalType
  )(using encoder: Encoder[T], notEnum: NotGiven[T <:< scala.reflect.Enum]): ToRowData[T] = new ToRowData[T] {
    // cache resolved schema
    private val encode: T => Any = encoder.encode(logicalType)

    def to(t: T, rowKind: RowKind): RowData = encode(t) match {
      case rowData: RowData =>
        rowData.setRowKind(rowKind)
        rowData
      case output =>
        val clazz = output.getClass
        throw new UnsupportedOperationException(
          s"Cannot marshall an instance of $t to RowData (had $clazz, output was $output)"
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
    override def encode(logicalType: LogicalType): U => Any = {
      val encodeT = self.encode(logicalType)
      u => encodeT(f(u))
    }
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
  override def join[T](ctx: CaseClass[Encoder, T]): Encoder[T] = DatatypeShape.of(ctx) match {
    case CaseClassShape.Record    => RowEncoder(ctx)
    case CaseClassShape.ValueType => RowEncoder(ctx)
    case CaseClassShape.Object    => ObjectEncoder(ctx)
  }

  override def split[T](ctx: SealedTrait[Encoder, T]): Encoder[T] =
    DatatypeShape.of[T](ctx) match {
      case SealedTraitShape.TypeUnion =>
        ctx.subtypes match {
          case IArray(single) => single.typeclass.asInstanceOf[Encoder[T]]
          case multiple       => TypeUnionEncoder(ctx)
        }
      case SealedTraitShape.Enum => EnumEncoder(ctx)
    }

// ==============================================
// TypeUnion   ==================================
// ==============================================

/** Encodes a sealed trait as a ROW with one nullable field per subtype (matched by name); only the field of the
  * actual subtype is set.
  */
class TypeUnionEncoder[T](ctx: SealedTrait[Encoder, T]) extends Encoder[T] {
  def encode(logicalType: LogicalType): T => Any = {
    require(logicalType.getTypeRoot == LogicalTypeRoot.ROW)
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala.toIndexedSeq
    val arity  = fields.length

    // resolved once per schema, by position in ctx.subtypes: the schema field of the subtype and its encoder
    val fieldIndexBySubtype = new Array[Int](ctx.subtypes.length)
    val encoderBySubtype    = new Array[T => Any](ctx.subtypes.length)
    ctx.subtypes.zipWithIndex.foreach { (st, position) =>
      val name = Names(st.typeInfo, new Annotations(st.annotations, st.inheritedAnnotations)).name
      val i    = fields.indexWhere(_.getName == name)
      if i == -1 then throw new IllegalArgumentException(s"Unable to find union field for subtype $name")
      fieldIndexBySubtype(position) = i
      encoderBySubtype(position) = st.typeclass.asInstanceOf[Encoder[T]].encode(fields(i).getType)
    }

    { value =>
      val position = Subtypes.positionOf(ctx, value)
      val row      = new GenericRowData(arity)
      row.setField(fieldIndexBySubtype(position), encoderBySubtype(position)(value).asInstanceOf[AnyRef])
      row
    }
  }
}

private[rowdata4s] object Subtypes:

  /** Position of the subtype of `value` in `ctx.subtypes`, the same scan as `ctx.choose` without the allocation.
    * `Subtype.index` is not used because it is not unique for nested sealed hierarchies.
    */
  def positionOf[F[_], T](ctx: SealedTrait[F, T], value: T): Int =
    val subtypes = ctx.subtypes
    var i        = 0
    while i < subtypes.length && !subtypes(i).cast.isDefinedAt(value) do i += 1
    if i == subtypes.length then
      throw new IllegalArgumentException(s"The given value `$value` is not a sub type of `${ctx.typeInfo.full}`")
    i

// ==============================================
// Enums   ======================================
// ==============================================

/** Encodes enums and sealed traits of case objects as their (annotation aware) name, the same name the decoder
  * matches on.
  */
class EnumEncoder[T](ctx: SealedTrait[Encoder, T]) extends Encoder[T] {
  def encode(logicalType: LogicalType): T => Any = {
    val encodeString = StringEncoder.encode(logicalType)
    // plain Strings by position in ctx.subtypes; StringData is not serializable and is created per record
    val namesBySubtype: Array[String] = ctx.subtypes.map { st =>
      Names(st.typeInfo, new Annotations(st.annotations, st.inheritedAnnotations)).name
    }.toArray
    { (value: T) => encodeString(namesBySubtype(Subtypes.positionOf(ctx, value))) }
  }
}

// ==============================================
// Objects   ======================================
// ==============================================

class ObjectEncoder[T](ctx: magnolia1.CaseClass[Encoder, T]) extends Encoder[T] {
  def encode(logicalType: LogicalType): T => Any = {
    val encodeString = StringEncoder.encode(logicalType)
    val name         = ctx.typeInfo.short
    { (value: T) => encodeString(name) }
  }
}

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
    // fields are written directly into the row, GenericRowData.of would copy them from a varargs array
    val length = encoders.length
    val row    = new GenericRowData(length)
    var i      = 0
    while i < length do {
      row.setField(i, encoders(i).encode(t).asInstanceOf[AnyRef])
      i += 1
    }
    row
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
  given Encoder[Long]                    = LongEncoder
  given Encoder[Int]                     = IntEncoder
  given Encoder[Short]                   = ShortEncoder
  given Encoder[Byte]                    = ByteEncoder
  given Encoder[Double]                  = DoubleEncoder
  given Encoder[Float]                   = FloatEncoder
  given BooleanEncoder: Encoder[Boolean] = Encoder(a => java.lang.Boolean.valueOf(a))
}

/** Converts to the integer column type the serializer expects: widening is lossless, narrowing wraps around. DATE
  * is stored as an int (epoch days) and is used by the LocalDate and Date encoders.
  */
object IntEncoder extends Encoder[Int]:
  override def encode(logicalType: LogicalType): Int => Any = logicalType.getTypeRoot match
    case TINYINT        => value => java.lang.Byte.valueOf(value.toByte)
    case SMALLINT       => value => java.lang.Short.valueOf(value.toShort)
    case INTEGER | DATE => value => java.lang.Integer.valueOf(value)
    case BIGINT         => value => java.lang.Long.valueOf(value.toLong)
    case _ =>
      throw new UnsupportedOperationException(s"IntEncoder doesn't support schema type ${logicalType.getTypeRoot}")

/** Converts to the floating point column type the serializer expects: Float into DOUBLE is lossless, Double into
  * FLOAT loses precision like a JVM cast.
  */
object FloatEncoder extends Encoder[Float]:
  override def encode(logicalType: LogicalType): Float => Any = logicalType.getTypeRoot match
    case FLOAT  => value => java.lang.Float.valueOf(value)
    case DOUBLE => value => java.lang.Double.valueOf(value.toDouble)
    case _ =>
      throw new UnsupportedOperationException(s"FloatEncoder doesn't support schema type ${logicalType.getTypeRoot}")

object DoubleEncoder extends Encoder[Double]:
  override def encode(logicalType: LogicalType): Double => Any = logicalType.getTypeRoot match
    case FLOAT  => value => java.lang.Float.valueOf(value.toFloat)
    case DOUBLE => value => java.lang.Double.valueOf(value)
    case _ =>
      throw new UnsupportedOperationException(s"DoubleEncoder doesn't support schema type ${logicalType.getTypeRoot}")

object LongEncoder extends Encoder[Long]:
  override def encode(logicalType: LogicalType): Long => Any = logicalType.getTypeRoot match
    case TINYINT  => value => java.lang.Byte.valueOf(value.toByte)
    case SMALLINT => value => java.lang.Short.valueOf(value.toShort)
    case INTEGER  => value => java.lang.Integer.valueOf(value.toInt)
    case BIGINT   => value => java.lang.Long.valueOf(value)
    case _ =>
      throw new UnsupportedOperationException(s"LongEncoder doesn't support schema type ${logicalType.getTypeRoot}")

/** The derived DataType maps Byte to INT, so the value is widened to the column type the serializer expects.
  */
object ByteEncoder extends Encoder[Byte]:
  override def encode(logicalType: LogicalType): Byte => Any = logicalType.getTypeRoot match
    case TINYINT  => value => java.lang.Byte.valueOf(value)
    case SMALLINT => value => java.lang.Short.valueOf(value.toShort)
    case INTEGER  => value => java.lang.Integer.valueOf(value.toInt)
    case BIGINT   => value => java.lang.Long.valueOf(value.toLong)
    case _ =>
      throw new UnsupportedOperationException(s"ByteEncoder doesn't support schema type ${logicalType.getTypeRoot}")

/** The derived DataType maps Short to INT, so the value is widened to the column type the serializer expects.
  */
object ShortEncoder extends Encoder[Short]:
  override def encode(logicalType: LogicalType): Short => Any = logicalType.getTypeRoot match
    case SMALLINT => value => java.lang.Short.valueOf(value)
    case INTEGER  => value => java.lang.Integer.valueOf(value.toInt)
    case BIGINT   => value => java.lang.Long.valueOf(value.toLong)
    case _ =>
      throw new UnsupportedOperationException(s"ShortEncoder doesn't support schema type ${logicalType.getTypeRoot}")

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
        (encoderK.encode(logicalType.asInstanceOf[MultisetType].getElementType), encoderV.encode(IntType(false)))
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
  given Encoder[BigDecimal] = BigDecimalEncoder

/** Encodes a BigDecimal with the precision and scale of the DECIMAL column. Flink serializes decimals as an unscaled
  * value and re-applies the column's scale when reading, so a [[DecimalData]] carrying any other scale would be read
  * back as a different number. Extra fractional digits are rounded HALF_UP like Flink's CAST; a value whose integer
  * part does not fit the column's precision cannot be represented and is rejected.
  */
object BigDecimalEncoder extends Encoder[BigDecimal]:
  override def encode(logicalType: LogicalType): BigDecimal => Any = logicalType match
    case decimalType: DecimalType =>
      val precision = decimalType.getPrecision
      val scale     = decimalType.getScale
      bd =>
        val decimal = DecimalData.fromBigDecimal(bd.underlying, precision, scale)
        if decimal == null then
          throw new IllegalArgumentException(s"BigDecimal $bd does not fit DECIMAL($precision, $scale)")
        decimal
    case _ => bd => DecimalData.fromBigDecimal(bd.underlying, bd.precision, bd.scale)

// ==============================================
// Bytes   ======================================
// ==============================================

trait ByteIterableEncoders:
  given Encoder[ByteBuffer]                                = ByteBufferEncoder
  given Encoder[Array[Byte]]                               = ByteArrayEncoder
  private val IterableByteEncoder: Encoder[Iterable[Byte]] = ByteArrayEncoder.contramap(_.toArray)
  given Encoder[List[Byte]]                                = IterableByteEncoder.contramap(identity)
  given Encoder[Vector[Byte]]                              = IterableByteEncoder.contramap(identity)
  given Encoder[Seq[Byte]]                                 = IterableByteEncoder.contramap(identity)

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
  // java.util.Date behaves like an instant (DataTypeFor maps it to TIMESTAMP_LTZ)
  given UtilDateEncoder: Encoder[java.util.Date] = InstantEncoder.contramap[java.util.Date](_.toInstant)

  given LocalDateEncoder: Encoder[LocalDate] = IntEncoder.contramap[LocalDate](_.toEpochDay.toInt)
  given Encoder[LocalTime]                   = LocalTimeEncoder
  given Encoder[LocalDateTime]               = LocalDateTimeEncoder

  given DateEncoder: Encoder[Date] = IntEncoder.contramap[Date](_.toLocalDate.toEpochDay.toInt)
  // an OffsetDateTime is an instant, the offset itself is not kept
  given OffsetDateTimeEncoder: Encoder[OffsetDateTime] = InstantEncoder.contramap[OffsetDateTime](_.toInstant)

object LocalTimeEncoder extends Encoder[LocalTime]:
  override def encode(logicalType: LogicalType): LocalTime => Any = {
    // Flink stores TIME_WITHOUT_TIME_ZONE as milliseconds-of-day as an int internally
    { value => java.lang.Integer.valueOf((value.toNanoOfDay / 1_000_000).toInt) }
  }

/** Flink stores TIMESTAMP columns with precision <= 3 as milliseconds only and its serializer asserts that the
  * nano-of-millisecond part is zero (it is silently dropped without `-ea`). Values are truncated to milliseconds for
  * those columns so that the behaviour does not depend on assertions being enabled.
  */
private[rowdata4s] object Timestamps:
  def precisionOf(logicalType: LogicalType): Int = logicalType match
    case t: TimestampType           => t.getPrecision
    case t: LocalZonedTimestampType => t.getPrecision
    case _                          => TimestampType.MAX_PRECISION

  def isCompact(logicalType: LogicalType): Boolean = TimestampData.isCompact(precisionOf(logicalType))

object InstantEncoder extends Encoder[Instant]:
  override def encode(logicalType: LogicalType): Instant => Any = {
    if Timestamps.isCompact(logicalType) then { value => TimestampData.fromEpochMillis(value.toEpochMilli) }
    else { value => TimestampData.fromInstant(value) }
  }

object LocalDateTimeEncoder extends Encoder[LocalDateTime]:
  private def epochMillis(temporal: LocalDateTime): Long = temporal.toInstant(ZoneOffset.UTC).toEpochMilli

  override def encode(logicalType: LogicalType): LocalDateTime => Any = {
    logicalType.getTypeRoot match
      case BIGINT => value => java.lang.Long.valueOf(epochMillis(value))
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE if Timestamps.isCompact(logicalType) =>
        value => TimestampData.fromEpochMillis(epochMillis(value))
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE =>
        value => TimestampData.fromLocalDateTime(value)
      case _ =>
        throw new UnsupportedOperationException(
          s"LocalDateTimeEncoder doesn't support schema type ${logicalType.getTypeRoot}"
        )
  }
