import sbt.Resolver

val JDK = "1.8"

val buildNumber =
  scala.util.Properties.envOrNone("version").map(v => "." + v).getOrElse("")
val hydraVersion = "0.11.3" + buildNumber
val jvmMaxMemoryFlag = sys.env.getOrElse("MAX_JVM_MEMORY_FLAG", "-Xmx2g")

lazy val defaultSettings = Seq(
  organization := "pluralsight",
  version := hydraVersion,
  scalaVersion := "2.12.11",
  description := "Hydra",
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  excludeDependencies += "org.slf4j" % "slf4j-log4j12",
  excludeDependencies += "log4j" % "log4j",
  dependencyOverrides ++= Seq(
    "org.apache.commons" % "commons-compress" % "1.24.0",
    "io.netty" % "netty-codec" % "4.1.77.Final",
//    "org.apache.zookeeper" % "zookeeper" % "3.7.2", -- snyk vulnerability fix
    "org.xerial.snappy" % "snappy-java" % "1.1.10.4",
    "org.apache.avro" % "avro" % Dependencies.avroVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.3",
    "org.apache.kafka" %% "kafka" % "2.8.2" % "test",
    "io.confluent" %% "kafka-schema-registry" % "6.2.1" % "test",
    "io.confluent" %% "kafka-avro-serializer" % "6.2.1" % "test",
  ),
  addCompilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.11.3" cross CrossVersion.full
  ),
  packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Implementation-Build" -> buildNumber),
  logLevel := Level.Info,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:_",
    "-deprecation",
    "-unchecked",
    "-Ypartial-unification"
  ),
  javacOptions in Compile ++= Seq(
    "-encoding",
    "UTF-8",
    "-source",
    JDK,
    "-target",
    JDK,
    "-Xlint:unchecked",
    "-Xlint:deprecation",
    "-Xlint:-options"
  ),
  resolvers += Resolver.mavenLocal,
  resolvers += "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  resolvers += "jitpack" at "https://jitpack.io",
  ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
  parallelExecution in sbt.Test := false,
  javaOptions in Universal ++= Seq(
    "-Dorg.aspectj.tracing.factory=default",
    "-J" + jvmMaxMemoryFlag
  )

)

lazy val restartSettings = Seq(
  javaOptions in reStart += jvmMaxMemoryFlag
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // required until these tickets are closed https://github.com/sbt/sbt-pgp/issues/42,
  // https://github.com/sbt/sbt-pgp/issues/36
  publishTo := Some(
    Resolver.file("Unused transient repository", file("target/unusedrepo"))
  ),
  packageBin := {
    new File("")
  },
  packageSrc := {
    new File("")
  },
  packageDoc := {
    new File("")
  }
)

lazy val moduleSettings =
  defaultSettings ++ Test.testSettings //++ Publish.settings

lazy val root = Project(
  id = "hydra",
  base = file(".")
)
  .settings(moduleSettings)
  .aggregate(common, core, avro, ingest, kafka)

lazy val common = Project(
  id = "common",
  base = file("common")
).settings(
  moduleSettings,
  name := "hydra-common",
  libraryDependencies ++= Dependencies.baseDeps
)

lazy val core = Project(
  id = "core",
  base = file("core")
).dependsOn(avro)
  .settings(
    moduleSettings,
    name := "hydra-core",
    libraryDependencies ++= Dependencies.coreDeps ++ Dependencies.awsAuthDeps ++ Dependencies.kafkaSchemaRegistryDep,
    dependencyOverrides ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.2",
      "org.apache.kafka" % "kafka-clients" % "2.8.2"
    )
  )

lazy val kafka = Project(
  id = "kafka",
  base = file("ingestors/kafka")
).dependsOn(core, common % "compile->compile;test->test")
  .configs(IntegrationTest)
  .settings(
    moduleSettings ++ Defaults.itSettings,
    name := "hydra-kafka",
    libraryDependencies ++= Dependencies.kafkaDeps,
    dependencyOverrides ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.2",
      "org.apache.kafka" % "kafka-clients" % "2.8.2"
    )
  )

lazy val avro = Project(
  id = "avro",
  base = file("avro")
).dependsOn(common)
  .settings(
    moduleSettings,
    name := "hydra-avro",
    libraryDependencies ++= Dependencies.avroDeps
  )

lazy val ingest = Project(
  id = "ingest",
  base = file("ingest")
)
  .dependsOn(core, kafka, common % "compile->compile;test->test")
  .settings(
    moduleSettings ++ dockerSettings,
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "hydra.ingest.bootstrap",
    buildInfoOptions += BuildInfoOption.ToJson,
    buildInfoOptions += BuildInfoOption.BuildTime,
    javaAgents += "org.aspectj" % "aspectjweaver" % "1.8.14",
    name := "hydra-ingest",
    libraryDependencies ++= Dependencies.ingestDeps
  )
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, JavaAgent, sbtdocker.DockerPlugin)

//scala style
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(sbt.Test).toTask("").value

lazy val dockerSettings = Seq(
  buildOptions in docker := BuildOptions(
    cache = false,
    removeIntermediateContainers = BuildOptions.Remove.Always,
    pullBaseImage = BuildOptions.Pull.IfMissing
  ),
  dockerfile in docker := {
    val appDir: File = stage.value
    val targetDir = "/app"

    new Dockerfile {
      from("java")
      maintainer("Alex Silva <alex-silva@pluralsight.com>")
      user("root")
      env("JAVA_OPTS", "-Xmx2G")
      runRaw("mkdir -p /etc/hydra")
      run("mkdir", "-p", "/var/log/hydra")
      expose(8088)
      entryPoint(s"$targetDir/bin/${executableScriptName.value}")
      copy(appDir, targetDir)
    }
  },
  imageNames in docker := Seq(
    // Sets the latest tag
    ImageName(s"${organization.value}/hydra:latest"),
    // Sets a name with a tag that contains the project version
    ImageName(
      namespace = Some(organization.value),
      repository = "hydra",
      tag = Some(version.value)
    )
  )
)

ThisBuild / githubWorkflowPublishTargetBranches := Seq.empty
