/*
  * Copyright 2015 MongoDB, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin._
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import scoverage.ScoverageSbtPlugin._

object MongoSparkBuild extends Build {

  import Dependencies._
  import Resolvers._

  val buildSettings = Seq(
    organization := "org.mongodb.spark",
    organizationHomepage := Some(url("http://www.mongodb.org")),
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.7",
    libraryDependencies ++= coreDependencies ++ testDependencies,
    resolvers := mongoSparkResolvers,
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature" /*, "-Xlog-implicits", "-Yinfer-debug", "-Xprint:typer"*/),
    scalacOptions in(Compile, doc) ++= Seq("-diagrams", "-unchecked", "-doc-root-content", "mongo-spark/rootdoc.txt")
  )

  val publishSettings = Publish.settings

  /*
   * Test Settings
   */
  val testSettings = Seq(
    testFrameworks += TestFrameworks.ScalaTest,
    testOptions in IntTest := Seq(Tests.Filter(itFilter)),
    testOptions in UnitTest <<= testOptions in Test,
    testOptions in UnitTest += Tests.Filter(unitFilter),
    ScoverageKeys.coverageMinimum := 95,
    ScoverageKeys.coverageFailOnMinimum := true
  ) ++ Seq(IntTest, UnitTest).flatMap {
    inConfig(_)(Defaults.testTasks)
  }

  def itFilter(name: String): Boolean = name endsWith "ISpec"

  def unitFilter(name: String): Boolean = !itFilter(name)

  lazy val IntTest = config("it") extend Test
  lazy val UnitTest = config("unit") extend Test

  val scoverageSettings = Seq()

  /*
   * Style and formatting
   */

  def scalariFormFormattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
  }

  val customScalariformSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := scalariFormFormattingPreferences,
    ScalariformKeys.preferences in Test    := scalariFormFormattingPreferences
  )

  val scalaStyleSettings = Seq(
    (scalastyleConfig in Compile) := file("project/scalastyle-config.xml")
  )

  /*
   * Assembly Jar Settings
   */
  val mongoSparkAssemblyJarSettings = assemblySettings ++
    addArtifact(Artifact("mongo-spark-alldep", "jar", "jar"), assembly) ++ Seq(test in assembly := {},
    excludedJars in assembly := {
      (fullClasspath in assembly).value filter {_.data.getName.startsWith("mongo-scala-driver-alldep")}
    })

  lazy val mongoSpark = Project(
    id = "mongo-spark",
    base = file("mongo-spark")
  ).configs(IntTest)
    .configs(UnitTest)
    .settings(buildSettings: _*)
    .settings(testSettings: _*)
    .settings(publishSettings: _*)
    .settings(mongoSparkAssemblyJarSettings: _*)
    .settings(customScalariformSettings: _*)
    .settings(scalaStyleSettings: _*)
    .settings(scoverageSettings: _*)
    .settings(initialCommands in console := """import org.mongodb.scala._""")

  lazy val sparked = Project(
    id = "sparked",
    base = file(".")
  ).aggregate(mongoSpark)
    .dependsOn(mongoSpark)
    .settings(buildSettings: _*)
    .settings(scalaStyleSettings: _*)
    .settings(scoverageSettings: _*)
    .settings(publish := {}, publishLocal := {})

  override def rootProject = Some(sparked)

}
