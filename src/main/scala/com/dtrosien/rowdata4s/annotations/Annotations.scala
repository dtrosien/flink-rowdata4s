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
}

case class TableName(override val name: String) extends TableNameable

case class TableTransient()  extends StaticAnnotation
case class TableErasedName() extends StaticAnnotation

trait TableNameable extends StaticAnnotation {
  val name: String
}

object Annotations {
  def apply(ctx: CaseClass[?, ?]): Annotations = new Annotations(ctx.annotations, ctx.inheritedAnnotations)
  def apply(annos: Seq[Any]): Annotations      = new Annotations(annos, Nil)
}
