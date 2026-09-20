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
import java.util.UUID
import scala.deriving.Mirror
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

  /** Reads column `index` of a row and decodes it. The default goes through the schema's [[RowData.FieldGetter]] and
    * [[decode]]; decoders for types with a typed [[RowData]] accessor override it to read the column directly, which
    * skips the boxing getter and the type test of [[decode]].
    */
  def decodeField(logicalType: LogicalType, index: Int): RowData => T = {
    val getter  = RowData.createFieldGetter(logicalType, index)
    val decodeT = decode(logicalType)
    row => decodeT(getter.getFieldOrNull(row))
  }

  final def map[U](f: T => U): Decoder[U] = new Decoder[U] {
    override def decode(logicalType: LogicalType): Any => U = {
      val decodeT = self.decode(logicalType)
      input => f(decodeT(input))
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => U = {
      val decodeT = self.decodeField(logicalType, index)
      row => f(decodeT(row))
    }
  }
}

private[rowdata4s] object Columns:
  /** The error the decoders raise for a null value, so the field fallback and the message stay the same. */
  def nullColumn(target: String): Nothing = throw new UnsupportedOperationException(s"Cannot convert null to type $target")

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

  /** Derivation for case classes. Magnolia's `rawConstruct` copies the decoded fields into a tuple before calling the
    * mirror; with the mirror at hand the row decoder constructs from the field array directly. Takes precedence over
    * the inherited Magnolia given because it is defined in the derived object.
    */
  inline given derivedProduct[T](using m: Mirror.ProductOf[T]): Decoder[T] =
    derived[T] match {
      case rowDecoder: RowDecoder[T] => rowDecoder.constructingWith(values => m.fromProduct(new ArrayProduct(values)))
      case other                     => other
    }
}

/** Adapts a field array to the [[Product]] a [[Mirror.ProductOf]] constructs from, without an intermediate tuple.
  * Public only because [[Decoder.derivedProduct]] is inline and references it at the call site; not part of the API.
  */
final class ArrayProduct(fields: Array[Any]) extends Product:
  def productArity: Int            = fields.length
  def productElement(n: Int): Any  = fields(n)
  def canEqual(that: Any): Boolean = false

// ==============================================
// Magnolia   ===================================
// ==============================================

trait MagnoliaDerivedDecoder extends AutoDerivation[Decoder]:

  override def join[T](ctx: CaseClass[Decoder, T]): Decoder[T] =
    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record => RowDecoder(ctx)
      case CaseClassShape.Object => ObjectDecoder(ctx)
    }

  override def split[T](ctx: SealedTrait[Decoder, T]): Decoder[T] =
    DatatypeShape.of(ctx)(_.isInstanceOf[ObjectDecoder[?]]) match {
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

/** Decodes a sealed trait from a ROW with one nullable field per subtype (matched by name); the non-null field is
  * the active one.
  */
class TypeUnionDecoder[T](ctx: magnolia1.SealedTrait[Decoder, T]) extends Decoder[T] {

  /** A sealed trait is read straight out of its union ROW column. */
  override def decodeField(logicalType: LogicalType, index: Int): RowData => T = {
    val decodeRow = decode(logicalType)
    val arity     = logicalType.asInstanceOf[RowType].getFieldCount
    row => if row.isNullAt(index) then Columns.nullColumn(ctx.typeInfo.full) else decodeRow(row.getRow(index, arity))
  }

  override def decode(logicalType: LogicalType): Any => T = {
    require(logicalType.getTypeRoot == LogicalTypeRoot.ROW)
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala.toIndexedSeq

    val subtypesByName: Map[String, SealedTrait.Subtype[Decoder, T, ?]] =
      ctx.subtypes.map(st => Names(st.typeInfo, new Annotations(st.annotations, st.inheritedAnnotations)).name -> st).toMap

    val getters: Array[RowData.FieldGetter] =
      fields.zipWithIndex.map { case (field, i) => RowData.createFieldGetter(field.getType, i) }.toArray
    val decoders: Array[Any => T] = fields.map { field =>
      subtypesByName.get(field.getName) match {
        case Some(st) => st.typeclass.asInstanceOf[Decoder[T]].decode(field.getType)
        case None     => _ => throw new RuntimeException(s"No subtype found for field ${field.getName}")
      }
    }.toArray
    val arity = fields.length

    { value =>
      val row = value.asInstanceOf[RowData]
      var i   = 0
      while i < arity && row.isNullAt(i) do i += 1
      if i == arity then throw new RuntimeException("All fields are null in union ROW")
      decoders(i)(getters(i).getFieldOrNull(row))
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
class RowDecoder[T](
    ctx: magnolia1.CaseClass[Decoder, T],
    construct: Array[Any] => T
) extends Decoder[T] {

  def this(ctx: magnolia1.CaseClass[Decoder, T]) = this(ctx, ctx.rawConstruct(_))

  /** The same decoder constructing the case class with the given function instead of Magnolia's `rawConstruct`. */
  def constructingWith(construct: Array[Any] => T): RowDecoder[T] = new RowDecoder[T](ctx, construct)

  /** A nested case class is read straight out of its ROW column. */
  override def decodeField(logicalType: LogicalType, index: Int): RowData => T = {
    val decodeRow = decode(logicalType)
    val arity     = logicalType.asInstanceOf[RowType].getFieldCount
    row => if row.isNullAt(index) then Columns.nullColumn(ctx.typeInfo.full) else decodeRow(row.getRow(index, arity))
  }

  override def decode(logicalType: LogicalType): Any => T = {
    val fields = logicalType.asInstanceOf[RowType].getFields.asScala

    // one decoder per case class parameter, in parameter order, so the values match the constructor
    val decoders = ctx.params.toList.map { param =>
      val paramName = new Annotations(param.annotations).name.getOrElse(param.label)
      fields.indexWhere(_.getName == paramName) match {
        case -1 => FieldDecoder.missing(param)
        case i  => FieldDecoder(param, fields(i).getType, i)
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
      construct(values)
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
      index: Int
  ): FieldDecoder[T] = new FieldDecoder[T] {
    private val decoder = param.typeclass.asInstanceOf[Decoder[T]].decodeField(logicalType, index)

    def decode(rowData: RowData): Any =
      try {
        decoder.apply(rowData)
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

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Byte = logicalType.getTypeRoot match
      case TINYINT  => row => if row.isNullAt(index) then Columns.nullColumn("BYTE") else row.getByte(index)
      case SMALLINT => row => if row.isNullAt(index) then Columns.nullColumn("BYTE") else row.getShort(index).toByte
      case INTEGER  => row => if row.isNullAt(index) then Columns.nullColumn("BYTE") else row.getInt(index).toByte
      case _        => super.decodeField(logicalType, index)
  }

  given Decoder[Short] = new BasicDecoder[Short] {
    override def decode(value: Any): Short = value match {
      case b: Byte  => b
      case s: Short => s
      case i: Int   => i.toShort
      case other    => throw new UnsupportedOperationException(s"Cannot convert $other to type SHORT")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Short = logicalType.getTypeRoot match
      case SMALLINT => row => if row.isNullAt(index) then Columns.nullColumn("SHORT") else row.getShort(index)
      case TINYINT  => row => if row.isNullAt(index) then Columns.nullColumn("SHORT") else row.getByte(index).toShort
      case INTEGER  => row => if row.isNullAt(index) then Columns.nullColumn("SHORT") else row.getInt(index).toShort
      case _        => super.decodeField(logicalType, index)
  }

  given IntDecoder: Decoder[Int] = new BasicDecoder[Int] {
    override def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => throw new UnsupportedOperationException(s"Cannot convert $other to type INT")
    }

    // DATE and TIME columns are ints as well (epoch days, millis of day), used by the LocalDate decoder
    override def decodeField(logicalType: LogicalType, index: Int): RowData => Int = logicalType.getTypeRoot match
      case INTEGER | DATE | TIME_WITHOUT_TIME_ZONE =>
        row => if row.isNullAt(index) then Columns.nullColumn("INT") else row.getInt(index)
      case SMALLINT => row => if row.isNullAt(index) then Columns.nullColumn("INT") else row.getShort(index).toInt
      case TINYINT  => row => if row.isNullAt(index) then Columns.nullColumn("INT") else row.getByte(index).toInt
      case _        => super.decodeField(logicalType, index)
  }

  given Decoder[Long] = new BasicDecoder[Long] {
    override def decode(value: Any): Long = value match {
      case byte: Byte   => byte.toLong
      case short: Short => short.toLong
      case int: Int     => int.toLong
      case long: Long   => long
      case other        => throw new UnsupportedOperationException(s"Cannot convert $other to type LONG")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Long = logicalType.getTypeRoot match
      case BIGINT   => row => if row.isNullAt(index) then Columns.nullColumn("LONG") else row.getLong(index)
      case INTEGER  => row => if row.isNullAt(index) then Columns.nullColumn("LONG") else row.getInt(index).toLong
      case SMALLINT => row => if row.isNullAt(index) then Columns.nullColumn("LONG") else row.getShort(index).toLong
      case TINYINT  => row => if row.isNullAt(index) then Columns.nullColumn("LONG") else row.getByte(index).toLong
      case _        => super.decodeField(logicalType, index)
  }

  given Decoder[Double] = new BasicDecoder[Double] {
    override def decode(value: Any): Double = value match {
      case d: Double => d
      case f: Float  => f.toDouble
      case other     => throw new UnsupportedOperationException(s"Cannot convert $other to type DOUBLE")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Double = logicalType.getTypeRoot match
      case DOUBLE => row => if row.isNullAt(index) then Columns.nullColumn("DOUBLE") else row.getDouble(index)
      case FLOAT  => row => if row.isNullAt(index) then Columns.nullColumn("DOUBLE") else row.getFloat(index).toDouble
      case _      => super.decodeField(logicalType, index)
  }

  given Decoder[Float] = new BasicDecoder[Float] {
    override def decode(value: Any): Float = value match {
      case f: Float => f
      case other    => throw new UnsupportedOperationException(s"Cannot convert $other to type FLOAT")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Float = logicalType.getTypeRoot match
      case FLOAT => row => if row.isNullAt(index) then Columns.nullColumn("FLOAT") else row.getFloat(index)
      case _     => super.decodeField(logicalType, index)
  }

  given Decoder[Boolean] = new BasicDecoder[Boolean] {
    override def decode(value: Any): Boolean = value match {
      case boolean: Boolean => boolean
      case other            => throw new UnsupportedOperationException(s"Cannot convert $other to type BOOLEAN")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Boolean = logicalType.getTypeRoot match
      case BOOLEAN => row => if row.isNullAt(index) then Columns.nullColumn("BOOLEAN") else row.getBoolean(index)
      case _       => super.decodeField(logicalType, index)
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
  override def decodeField(logicalType: LogicalType, index: Int): RowData => String = logicalType.getTypeRoot match
    case CHAR | VARCHAR =>
      row => if row.isNullAt(index) then Columns.nullColumn("String") else row.getString(index).toString
    case _ => super.decodeField(logicalType, index)

  override def decode(logicalType: LogicalType): Any => String = {
    case stringData: StringData => stringData.toString
    case string: String         => string
    case charseq: CharSequence  => charseq.toString
    case other: Any =>
      throw new UnsupportedOperationException(s"Unsupported type $other ${other.getClass} for StringDecoder")
  }

object CharSequenceDecoder extends Decoder[CharSequence]:
  override def decodeField(logicalType: LogicalType, index: Int): RowData => CharSequence =
    StringDecoder.decodeField(logicalType, index)

  override def decode(logicalType: LogicalType): Any => CharSequence = {
    case stringData: StringData => stringData.toString
    case string: String         => string
    case charseq: CharSequence  => charseq
    case other: Any =>
      throw new UnsupportedOperationException(s"Unsupported type $other ${other.getClass} for StringDecoder")
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

  override def decodeField(logicalType: LogicalType, index: Int): RowData => Option[T] = {
    require(logicalType.isNullable, "Options can only be decoded when type is nullable")

    val decodeT = decoder.decodeField(logicalType, index)
    { row => if row.isNullAt(index) then None else Some(decodeT(row)) }
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
      // Flink stores MULTISET<T> as a map from element to count (INT), i.e. a Map[T, Int]
      case MULTISET =>
        val keyType   = logicalType.asInstanceOf[MultisetType].getElementType
        val valueType = IntType(false)
        (keyType, valueType, decoderK.decode(keyType), decoderV.decode(valueType))
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
    override def decodeField(logicalType: LogicalType, index: Int): RowData => BigDecimal = logicalType match
      case decimalType: DecimalType =>
        val precision = decimalType.getPrecision
        val scale     = decimalType.getScale
        row =>
          if row.isNullAt(index) then Columns.nullColumn("BigDecimal")
          else BigDecimal(row.getDecimal(index, precision, scale).toBigDecimal)
      case _ => super.decodeField(logicalType, index)

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
  // java.util.Date behaves like an instant (DataTypeFor maps it to TIMESTAMP_LTZ)
  given UtilDateDecoder: Decoder[java.util.Date]     = InstantDecoder.map[java.util.Date](java.util.Date.from)
  given DateDecoder: Decoder[Date]                   = LocalDateDecoder.map[Date](Date.valueOf)
  given LocalDateTimeDecoder: Decoder[LocalDateTime] = InstantDecoder.map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
  given LocalTimeDecoder: Decoder[LocalTime] = new Decoder[LocalTime] {
    override def decode(logicalType: LogicalType): Any => LocalTime = {
      // Flink stores TIME_WITHOUT_TIME_ZONE as milliseconds-of-day as an int internally
      case i: Int  => LocalTime.ofNanoOfDay(i.toLong * 1_000_000)
      case l: Long => LocalTime.ofNanoOfDay(l * 1_000_000)
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => LocalTime = logicalType.getTypeRoot match
      case TIME_WITHOUT_TIME_ZONE | INTEGER =>
        row =>
          if row.isNullAt(index) then Columns.nullColumn("LocalTime")
          else LocalTime.ofNanoOfDay(row.getInt(index).toLong * 1_000_000)
      case _ => super.decodeField(logicalType, index)
  }
  given LocalDateDecoder: Decoder[LocalDate] = Decoder.IntDecoder.map[LocalDate](i => LocalDate.ofEpochDay(i.toLong))

  // an OffsetDateTime is an instant, it is read back with offset UTC
  given OffsetDateTimeDecoder: Decoder[OffsetDateTime] = InstantDecoder.map(OffsetDateTime.ofInstant(_, ZoneOffset.UTC))

  given InstantDecoder: Decoder[Instant] = new Decoder[Instant] {
    override def decode(logicalType: LogicalType): Any => Instant = {
      case timestampData: TimestampData => timestampData.toInstant
      case l: Long                      => Instant.ofEpochMilli(l)
      case i: Int                       => Instant.ofEpochMilli(i.toLong)
      case other => throw new IllegalArgumentException(s"Unsupported type for Instant decoding: ${other.getClass}")
    }

    override def decodeField(logicalType: LogicalType, index: Int): RowData => Instant = logicalType match
      case t: TimestampType           => timestamp(index, t.getPrecision)
      case t: LocalZonedTimestampType => timestamp(index, t.getPrecision)
      case t if t.getTypeRoot == BIGINT =>
        row => if row.isNullAt(index) then Columns.nullColumn("Instant") else Instant.ofEpochMilli(row.getLong(index))
      case _ => super.decodeField(logicalType, index)

    private def timestamp(index: Int, precision: Int): RowData => Instant =
      row => if row.isNullAt(index) then Columns.nullColumn("Instant") else row.getTimestamp(index, precision).toInstant
  }


// ==============================================
// Utils   ===================================
// ==============================================

/** Thrown when a field of a [[RowData]] cannot be decoded; names the field and the column type, the cause is the
  * underlying error.
  */
class RowDataDecodingException(message: String, cause: Throwable) extends RuntimeException(message, cause)