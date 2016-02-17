import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

name := "snapshotter"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.socrata"               %% "socrata-http-jetty"      % "3.4.1" excludeAll(
    ExclusionRule(organization = "com.socrata", name = "socrata-http-common")),
  "com.socrata"               %% "socrata-http-client"     % "3.4.1" excludeAll(
    ExclusionRule(organization = "com.socrata", name = "socrata-http-common")),
  "com.socrata"               %% "socrata-http-common"     % "3.4.1" excludeAll(
    ExclusionRule(organization = "com.rojoma")),
  "com.socrata"               %% "socrata-curator-utils"   % "1.0.3" excludeAll(
    ExclusionRule(organization = "com.socrata", name = "socrata-http-client"),
    ExclusionRule(organization = "com.socrata", name = "socrata-http-jetty")),
  "ch.qos.logback"             % "logback-classic"         % "1.1.3",
  "com.amazonaws"              % "aws-java-sdk-s3"         % "1.9.0" excludeAll(
    ExclusionRule(organization = "commons-logging", name = "commons-logging")),
  "org.apache.curator"         % "curator-x-discovery"     % "2.8.0"
)

// Test dependencies

libraryDependencies ++= Seq(
  "org.mockito"               % "mockito-core"             %"1.10.19" % "test"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

enablePlugins(sbtbuildinfo.BuildInfoPlugin)

com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Test := {}
com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Compile := {}

// Setup revolver.
Revolver.settings

val LastVerDate = """^(\d+\.\d+\.\d+)(?:\D.*)?$""".r

releaseVersion := { lastVerRaw =>
  // We want three-segment versions unless we release more than once in a day, in which
  // case we'll append the current time as a submicro component.
  val now = DateTime.now()
  val optimisticNextVer = DateTimeFormat.forPattern("yyyy.MM.dd").withZoneUTC.print(now)
  lastVerRaw match {
    case LastVerDate(lastVer) if lastVer == optimisticNextVer =>
      optimisticNextVer + "." + DateTimeFormat.forPattern("HHmm").withZoneUTC.print(now)
    case _ =>
      optimisticNextVer
  }
}

releaseNextVersion := { lastVer => lastVer + "-DEVELOPMENT" }
