ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "Day28-Real-Time-Booking-Project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"      % sparkVersion,
      "org.apache.spark" %% "spark-sql"       % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.scalatest"    %% "scalatest"       % "3.2.18" % Test
    ),
    // Spark needs a forked JVM (and these flags on Java 17+) when started from sbt.
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Compile / mainClass := Some("com.day28.booking.BookingStreamingApp")
  )
