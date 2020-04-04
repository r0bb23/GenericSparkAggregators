import Versions._
import sbt._

object Dependencies {
    val sparkDeps = Seq(
        "org.apache.spark" %% "spark-core" % sparkVer % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
        "com.holdenkarau" %% "spark-testing-base" % s"${sparkVer}_${sparkTestVer}" % "test",
    )

    val generalDeps = Seq(
        "org.scalatest" %% "scalatest" % scalaTestVer,
    )

    val analyticsDeps = Seq(
        "org.scalanlp" %% "breeze" % breezeVer,
        "com.tdunning" % "t-digest" % tdigestVer,
        "com.yahoo.datasketches" % "sketches-core" % yahooSketches,
        "com.github.halfmatthalfcat" %% "stringmetric-core" % stringmetricVer,
        "io.sgr" % "s2-geometry-library-java" % s2GeometryVer,
        "com.datadoghq" % "sketches-java" % ddSketchVer,
    )

    val repos = Seq(
        "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
        "Java.net Maven2 Repository" at "https://download.java.net/maven/2/"
    )
}