package com.dtrosien.rowdata4s.annotations

import com.dtrosien.rowdata4s.*
import magnolia1.CaseClass

import scala.annotation.StaticAnnotation

class Annotations(annos: Seq[Any], inheritedAnnos: Seq[Any] = Nil) {
  private[Annotations] val allAnnos: Seq[Any] = annos ++ inheritedAnnos

  def name: Option[String] = annos.collectFirst { case t: TableNameable =>
    t.name
  }

  def erased: Boolean = annos.collectFirst { case t: TableErasedName =>
    t
  }.isDefined

  def transient: Boolean = annos.collectFirst { case t: TableTransient =>
    t
  }.isDefined

  def decimal: Option[TableDecimal] = annos.collectFirst { case t: TableDecimal => t }

  def timestampPrecision: Option[Int] = annos.collectFirst { case t: TableTimestampPrecision => t.precision }

  def comment: Option[String] = annos.collectFirst { case t: TableComment => t.text }

  def varchar: Option[Int] = annos.collectFirst { case t: TableVarchar => t.length }

  def char: Option[Int] = annos.collectFirst { case t: TableChar => t.length }
}

/** Renames the column of a field, or the union field of a sealed trait / enum case. */
case class TableName(override val name: String) extends TableNameable

/** Leaves the field out of the derived schema. */
case class TableTransient() extends StaticAnnotation

case class TableErasedName() extends StaticAnnotation

/** Precision and scale of the DECIMAL column of a BigDecimal field; overrides the [[ScalePrecision]] given. */
case class TableDecimal(precision: Int, scale: Int) extends StaticAnnotation

/** Precision of the TIMESTAMP / TIMESTAMP_LTZ column of a temporal field; overrides the [[TimestampPrecision]] given.
  */
case class TableTimestampPrecision(precision: Int) extends StaticAnnotation

/** Description of the column, e.g. the column doc of an Iceberg table. */
case class TableComment(text: String) extends StaticAnnotation

/** `VARCHAR(length)` instead of `STRING` for a String field; longer values are truncated on encode. */
case class TableVarchar(length: Int) extends StaticAnnotation

/** `CHAR(length)` instead of `STRING` for a String field; values are truncated or padded with spaces on encode. */
case class TableChar(length: Int) extends StaticAnnotation

trait TableNameable extends StaticAnnotation {
  val name: String
}

object Annotations {
  def apply(ctx: CaseClass[?, ?]): Annotations = new Annotations(ctx.annotations, ctx.inheritedAnnotations)
  def apply(annos: Seq[Any]): Annotations      = new Annotations(annos, Nil)
}
