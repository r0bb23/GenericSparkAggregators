import Versions._
import sbt._

object Dependencies {
    val sparkDeps = Seq(
        "org.apache.spark" %% "spark-core" % sparkVer % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
        "com.holdenkarau" %% "spark-testing-base" % s"${sparkVer}_0.12.0" % "test",
    )

    val generalDeps = Seq(
        "org.scalatest" %% "scalatest" % scalaTestVer,
    )

    val analyticsDeps = Seq(
        "org.scalanlp" %% "breeze" % breezeVer,
        "mozilla" % "spark-hyperloglog" % hyperloglogVer,
        "com.tdunning" % "t-digest" % tdigestVer,
        "com.yahoo.datasketches" % "sketches-core" % yahooSketches,
        "com.twitter" %% "algebird-core" % algebirdVer,
        "com.rockymadden.stringmetric" %% "stringmetric-core" % stringmetricVer,
        "io.sgr" % "s2-geometry-library-java" % s2GeometryVer,
    )
    
    val repos = Seq(
        "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
        "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
        "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/",
    )
}