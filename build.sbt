name := "eth-graph"

version := "1.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-graphx" % "2.2.1"
)

mainClass in (Compile,run) := Some("com.github.ethgraph.jobs.ssp.ShortestPathJob")
mainClass in (Compile, packageBin) := Some("com.github.ethgraph.jobs.ssp.ShortestPathJob")