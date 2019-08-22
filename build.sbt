import Dependencies.{ generalDeps, sparkDeps, analyticsDeps, testDeps }
import Versions.{ gsaggsVer, scalaVer }
import scala.sys.process.Process

lazy val commonSettings: Seq[Def.Setting[_]] = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.rbb",
  version := gsaggsVer,
  scalaVersion in ThisBuild := scalaVer,
  resolvers ++= Dependencies.repos,
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value,
  parallelExecution in Test := false,
  scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature"),
  javaOptions in Test ++= Seq(
    "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer",
    "-Dspark.kryo.registrationRequired=true",
    "-Dspark.kryo.registrator=com.rbb.genericsparkaggregators.RbbKryoRegistrator"
  ),
  ivyXML :=
    <dependencies>
    <exclude module="jms"/>
    <exclude module="jmxtools"/>
    <exclude module="jmxri"/>
    </dependencies>,
  scalariformAutoformat := false
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
    Assembly.settings,
    name := "report-analyses",
    libraryDependencies ++= generalDeps ++ sparkDeps ++ analyticsDeps ++ testDeps,
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
    // Can import as 'com.rbb.genericsparkaggregators.BuildInfo'
    buildInfoPackage := "com.rbb.genericsparkaggregators"
)

lazy val assemblyJarPath = s"${parentPath}/commands/spark/"
lazy val assemblyJarFilename = s"gsaggs_2.11-${gsaggsVer}.jar"

assemblyOutputPath in assembly := file(assemblyJarPath + assemblyJarFilename)

publishTo := Some(Resolver.file("file",  new File(mavenRepo)))
