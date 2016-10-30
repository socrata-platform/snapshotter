// tells sbt where to look to resolve versions
resolvers ++= Seq("sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
                  "Socrata Cloudbees" at "https://repo.socrata.com/artifactory/libs-release")

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.6.1")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
