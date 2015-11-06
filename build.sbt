name := "snapshotter"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.socrata"               %% "socrata-http-jetty"      % "3.4.1",
  "com.socrata"               %% "socrata-http-client"     % "3.4.1",
  "com.socrata"               %% "socrata-http-common"     % "3.4.1",
  "com.socrata"               %% "socrata-curator-utils"   % "1.0.3",
  "ch.qos.logback"             % "logback-classic"         % "1.1.3",
  "com.amazonaws"              % "aws-java-sdk-s3"         % "1.9.0",
  "org.apache.curator"         % "curator-x-discovery"     % "2.8.0"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

enablePlugins(sbtbuildinfo.BuildInfoPlugin)

// Setup revolver.
Revolver.settings
