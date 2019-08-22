import Versions._

object Dependencies {
    val sparkDeps = Seq(
        "org.apache.spark" %% "spark-core" % sparkVer % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
    )

    val generalDeps = Seq(
        "org.scalatest" %% "scalatest" % scalaTestVer % "test",
    )

    val analyticsDeps = Seq(
        "org.scalanlp" %% "breeze" % breezeVer,
        "com.tdunning" % "t-digest" % tdigestVer,
        "com.yahoo.datasketches" % "sketches-core" % yahooSketches,
        "com.twitter" %% "algebird-core" % algebirdVer,
        "com.rockymadden.stringmetric" %% "stringmetric-core" % stringmetricVer,
        "io.sgr" % "s2-geometry-library-java" % s2GeometryVer,
    )
}