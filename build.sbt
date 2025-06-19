Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.dtrosien.rowdata4s"
ThisBuild / scalaVersion := "3.6.2"

scalacOptions += "-Xmax-inlines:64"

lazy val root = (project in file(".")).settings(
  name := "flink-rowdata4s",
  libraryDependencies ++= Seq(
    "com.sksamuel.avro4s" %% "avro4s-core"          % "5.0.13",
    "org.apache.flink"     % "flink-table-api-java" % "1.20.0" % Provided,
    "org.scalatest"       %% "scalatest"            % "3.2.19" % Test,
    "org.apache.flink"     % "flink-avro"           % "1.20.0" % Test,
    "ch.qos.logback"       % "logback-classic"      % "1.5.17" % Test
  )
)
