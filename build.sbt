name := "snapshotter"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.socrata"        %% "socrata-http-jetty"      % "3.4.1",
  "ch.qos.logback"      % "logback-classic"         % "1.1.3"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

enablePlugins(sbtbuildinfo.BuildInfoPlugin)

// Setup revolver.
Revolver.settings
