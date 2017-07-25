name := "spark-wrapper"

version := "0.1-SNAPSHOT"

organization := "org.me"

scalaVersion := "2.11.11"

exportJars := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"

resolvers += "Will's bintray" at "https://dl.bintray.com/willb/maven/"
// libraryDependencies += "com.redhat.et" %% "silex" % "0.1.2"

libraryDependencies += "com.redhat.et" %% "silex" % "0.1.2" % "provided"

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)
