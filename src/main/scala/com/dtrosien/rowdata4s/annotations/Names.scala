package com.dtrosien.rowdata4s.annotations

import magnolia1.TypeInfo

/** Extracts names and namespaces from a type. Takes into consideration provided annotations.
  */
case class Names(typeInfo: TypeInfo, annos: Annotations) {

  private val defaultNamespace = typeInfo.owner.replaceAll("\\.<local .*?>", "").stripSuffix(".package")

  // the name of the scala class without type parameters.
  // Eg, List[Int] would be List.
  private val erasedName = typeInfo.short

  // the name of the scala class with type parameters encoded,
  // Eg, List[Int] would be `List__Int`
  // Eg, Type[A, B] would be `Type__A_B`
  // this method must also take into account @TableName on the classes used as type arguments
  private val genericName = {
    if typeInfo.typeParams.isEmpty then {
      erasedName
    } else {
      val targs = typeInfo.typeParams.map { tparam => Names(tparam).name }.mkString("_")
      typeInfo.short + "__" + targs
    }
  }

  /** Returns the record name for this type to be used when creating row data. This method takes into account type
    * parameters and annotations.
    *
    * The general format for a record name is `resolved-name__typea_typeb_typec`. That is a double underscore delimits
    * the resolved name from the start of the type parameters and then each type parameter is delimited by a single
    * underscore.
    *
    * The name is the class name with any annotations applied, such as @TableName or @TableErasedName, which, if
    * present, means the type parameters will not be included in the final name.
    */
  def name: String = annos.name.getOrElse {
    if annos.erased then erasedName else genericName
  }

  private def namespace: String = defaultNamespace

  /** Returns the full record name (namespace + name) taking into account annotations and type parameters.
    */
  def fullName: String = namespace.trim() match {
    case ""        => name
    case otherwise => namespace + "." + name
  }

}

object Names {
  def apply(info: TypeInfo): Names = Names(info, Annotations(Nil))
}
