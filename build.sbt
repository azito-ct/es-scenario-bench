ThisBuild / scalaVersion := "3.2.1"

name := "es-scenario-bench"

libraryDependencies ++= Seq(
    "com.softwaremill.sttp.client3" %% "core" % "3.8.8",
    "com.softwaremill.sttp.client3" %% "circe" % "3.8.8",
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-cats" % "3.8.8",

    "io.circe" %% "circe-core" % "0.14.1",
    "io.circe" %% "circe-parser" % "0.14.1",
    "io.circe" %% "circe-generic" % "0.14.1",

    "org.typelevel" %% "cats-effect" % "3.4.5"
)

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.8.2" cross(CrossVersion.for3Use2_13)