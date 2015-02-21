name := "spark-getting-started"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.5.1"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha2"
