// import AssemblyKeys._

name := "spark-examples"

organization := "org.collamine"

version := "0.1.0"

scalaVersion := "2.11.3"

// crossScalaVersions := Seq("2.9.2", "2.10.3", "2.11.3")

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Maven Repository" at "http://mvnrepository.com/artifact/"

resolvers += "luzhuomi github repo" at "https://raw.githubusercontent.com/luzhuomi/mavenrepo/master/"



scalacOptions += "-deprecation"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "com.github.luzhuomi" %% "scalapderiv" % "0.0.8"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.4" // scala license

libraryDependencies += "com.github.luzhuomi" %% "scalazparsec" % "0.1.4"  // apache license


assemblyMergeStrategy in assembly := {

    case PathList("log4j.properties") => MergeStrategy.discard
    case PathList("defaults.yaml", xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("LICENSE", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.last // leiningen build files

}
