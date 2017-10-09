// import AssemblyKeys._

// assemblySettings

name := "spark-ml-examples"

version := "0.1"

scalaVersion := "2.11.3"

// scalaVersion := "2.10.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.8.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"


assemblyMergeStrategy in assembly := {

    case PathList("log4j.properties") => MergeStrategy.discard
    case PathList("defaults.yaml", xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("LICENSE", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.last // leiningen build files

}
