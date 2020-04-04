import scala.sys.process.Process
import org.scoverage.coveralls.Imports.CoverallsKeys._

lazy val commonSettings: Seq[Def.Setting[_]] = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.rbb",
  version := Versions.gsaggsVer,
  scalaVersion in ThisBuild := Versions.scalaVer,
  resolvers ++= Dependencies.repos,
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  dependencyOverrides += "org.scala-lang" % "scala-library" % Versions.scalaVer,
  parallelExecution in Test := false,
  scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature"),
  ivyXML :=
    <dependencies>
    <exclude module="jms"/>
    <exclude module="jmxtools"/>
    <exclude module="jmxri"/>
    </dependencies>,
  scalariformAutoformat := false,
  coverageEnabled := true,
  coverallsToken := sys.env.get("COVERALLS_GSAGGS_TOKEN"),
)

lazy val rootSettings = Seq(
  /** https://github.com/holdenk/spark-testing-base#special-considerations **/
  parallelExecution in Test := false,
)

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    rootSettings,
    name := "gsaggs",
    libraryDependencies ++= Dependencies.analyticsDeps ++ Dependencies.generalDeps ++ Dependencies.sparkDeps,
    test in assembly := {},
    fork in Test := true,
    // Full stack traces + timings
    testOptions in Test += Tests.Argument("-oDF"),
    // Make spark available in sbt console
    // https://sanori.github.io/2017/06/Using-sbt-instead-of-spark-shell/
    // http://spark.apache.org/docs/latest/#running-the-examples-and-shell
    initialCommands in console := """
      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.functions._
      import org.apache.log4j.{ Level, Logger }

      // Turn off verbose logging
      Logger.getLogger("akka").setLevel(Level.OFF)

      val spark = SparkSession.builder()
        .master("local[2]")
        .appName("spark-shell")
        .getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext
    """,
    cleanupCommands in console := "spark.stop()",
    // File is generated into: target/scala-2.11/src_managed/main/sbt-buildinfo/BuildInfo.scala
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion) ++ Seq[BuildInfoKey](
      BuildInfoKey.action("buildTime") {
        System.currentTimeMillis
      },
      BuildInfoKey.action("gitHash") {
        (Process("git rev-parse HEAD")!!).stripLineEnd
      },
      BuildInfoKey.action("gitBranch") {
        (Process("git branch")!!).split("\n").filter(_.startsWith("*")).last.split(" +").last
      },
      BuildInfoKey.action("hostName") {
        (Process("hostname")!!).stripLineEnd
      }
    ),
    // Can import as 'com.rbb.gsaggs.BuildInfo'
    buildInfoPackage := "com.rbb.gsaggs"
)

val parentPath = file(".").getAbsoluteFile.getParentFile
lazy val assemblyJarPath = s"${parentPath}/commands/spark/"
lazy val assemblyJarFilename = s"gsaggs_2.12-${Versions.gsaggsVer}.jar"

assemblyOutputPath in assembly := file(assemblyJarPath + assemblyJarFilename)