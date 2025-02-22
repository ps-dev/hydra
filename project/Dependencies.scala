import sbt.{ExclusionRule, _}

object Dependencies {

  val akkaHTTPCorsVersion = "1.0.0"
  val akkaHTTPVersion = "10.1.13"
  val akkaKafkaStreamVersion = "2.0.4"
  val akkaVersion = "2.6.7"
  val avroVersion = "1.11.4"
  val catsEffectVersion = "2.4.0"
  val catsLoggerVersion = "1.2.1"
  val catsRetryVersion = "2.1.0"
  val catsVersion = "2.4.2"
  val cirisVersion = "1.2.1"
  val confluentVersion = "6.2.1"
  val fs2KafkaVersion = "1.4.1"
  val jacksonCoreVersion = "2.10.4"
  val jacksonDatabindVersion = "2.10.4"
  val jodaConvertVersion = "2.2.3"
  val jodaTimeVersion = "2.12.5"
  val kafkaVersion = "2.8.2"
  val kamonPVersion = "2.1.10"
  val kamonVersion = "2.1.10"
  val log4jVersion = "2.22.1"
  val refinedVersion = "0.9.20"
  val reflectionsVersion = "0.9.12"
  val scalaCacheVersion = "0.28.0"
  val scalaMockVersion = "5.1.0"
  val scalaTestVersion = "3.2.3"
  val sprayJsonVersion = "1.3.6"
  val testContainersVersion = "0.38.8"
  val typesafeConfigVersion = "1.3.2"
  val vulcanVersion = "1.2.0"
  val scalaTestEmbeddedRedisVersion = "0.4.0"
  val scalaChillBijectionVersion = "0.10.0"
  val awsSdkVersion = "2.17.192"
  val enumeratumVersion = "1.7.2"

  object Compile {

    val refined = "eu.timepit" %% "refined" % refinedVersion

    val vulcan: Seq[ModuleID] = Seq(
      "com.github.fd4s" %% "vulcan",
      "com.github.fd4s" %% "vulcan-generic",
      "com.github.fd4s" %% "vulcan-refined"
    ).map(_ % vulcanVersion)

    val cats = Seq(
      "com.github.cb372" %% "cats-retry" % catsRetryVersion,
      "org.typelevel" %% "log4cats-core" % catsLoggerVersion,
      "org.typelevel" %% "log4cats-slf4j" % catsLoggerVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )

    lazy val catsEffect = Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "com.github.cb372" %% "cats-retry" % catsRetryVersion
    )

    val fs2Kafka = Seq(
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion
    )

    val ciris = "is.cir" %% "ciris" % cirisVersion


    val typesafeConfig = "com.typesafe" % "config" % typesafeConfigVersion

    val sprayJson = "io.spray" %% "spray-json" % sprayJsonVersion

    val retry = "com.softwaremill.retry" %% "retry" % "0.3.3"

    val embeddedKafka =
      "io.github.embeddedkafka" %% "embedded-kafka" % "2.8.1" % "test"

    lazy val kamon = Seq(
      "io.kamon" %% "kamon-core" % kamonVersion,
      "io.kamon" %% "kamon-prometheus" % kamonPVersion
    )

    val kafkaClients: Seq[ModuleID] =  Seq("org.apache.kafka" % "kafka-clients" % kafkaVersion)

    val kafka: Seq[ModuleID] = Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      embeddedKafka
    ) ++ kafkaClients

    val kafkaAvroSerializer: Seq[ModuleID] =
      Seq("io.confluent" % "kafka-avro-serializer" % confluentVersion).map(
        _.excludeAll(
          ExclusionRule(organization = "org.codehaus.jackson"),
          ExclusionRule(organization = "com.fasterxml.jackson.core"),
          ExclusionRule(organization = "org.apache.kafka")
        )
      )

    val kafkaSchemaRegistry: Seq[ModuleID] = Seq("io.confluent" % "kafka-schema-registry-client" % confluentVersion).map(
      _.excludeAll(
        ExclusionRule(organization = "org.scala-lang.modules"),
        ExclusionRule(organization = "org.apache.kafka", "kafka-clients"),
        ExclusionRule(organization = "com.fasterxml.jackson.module"),
        ExclusionRule(organization = "org.scala-lang.modules"),
        ExclusionRule(organization = "com.typesafe.scala-logging")
      )
    )

    val awsMskIamAuth = Seq("software.amazon.msk" % "aws-msk-iam-auth" % "1.1.4")

    val awsSdk = Seq(
      "software.amazon.awssdk" % "iam" % awsSdkVersion,
      "software.amazon.awssdk" % "arns" % awsSdkVersion
    )

    val logging = Seq(
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion
    )

    val akka = Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,
      "ch.megard" %% "akka-http-cors" % akkaHTTPCorsVersion,
      "org.iq80.leveldb" % "leveldb" % "0.7",
      "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
    )

    val akkaHttpHal = Seq(
      ("com.github.marcuslange" % "akka-http-hal" % "1.2.5")
        .excludeAll(ExclusionRule(organization = "io.spray"))
    )


    val akkaKafkaStream =
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaStreamVersion

    val avro = "org.apache.avro" % "avro" % avroVersion

    val joda = Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.joda" % "joda-convert" % jodaConvertVersion
    )

    val guavacache =
      Seq(
        "com.github.cb372" %% "scalacache-guava",
        "com.github.cb372" %% "scalacache-cats-effect"
      ).map(_ % scalaCacheVersion)

    val redisCache =
      Seq(
        "com.twitter" %% "chill-bijection" % scalaChillBijectionVersion,
        "com.github.cb372" %% "scalacache-redis" % scalaCacheVersion
      )

    val reflections = "org.reflections" % "reflections" % reflectionsVersion

    val jackson = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonCoreVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion
    )

    val enumeratum = "com.beachape" %% "enumeratum" % enumeratumVersion
  }

  // oneOf test, it, main
  object TestLibraries {

    def getTestLibraries(module: String): Seq[ModuleID] = {
      val akkaTest = Seq(
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % module,
        "com.typesafe.akka" %% "akka-http-testkit" % akkaHTTPVersion % module,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % module
      )

      val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % module

      val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion % module
      val junit = "junit" % "junit" % "4.13.1" % module

      val embeddedKafkaSchemaRegistry =
        "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" %  confluentVersion  % module

      val scalatestEmbeddedRedis = "com.github.sebruck" %% "scalatest-embedded-redis" % scalaTestEmbeddedRedisVersion % module

      akkaTest ++ Seq(scalaTest, scalaMock, junit, scalatestEmbeddedRedis, embeddedKafkaSchemaRegistry)
    }

  }

  object Integration {
    private val testcontainersJavaVersion = "1.15.1"
    val testContainers = Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testContainersVersion % "it",
      "com.dimafeng" %% "testcontainers-scala-kafka" % testContainersVersion % "it",
      "org.testcontainers" % "testcontainers"                  % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "database-commons"                % testcontainersJavaVersion  % "it",
      "org.testcontainers" % "jdbc"                            % testcontainersJavaVersion  % "it"
    )

  }

  import Compile._
  import Test._
  import Integration._

  val testDeps: Seq[ModuleID] = TestLibraries.getTestLibraries(module = "test")

  val integrationDeps: Seq[ModuleID] = testContainers ++ TestLibraries.getTestLibraries(module = "it")

  val baseDeps: Seq[ModuleID] =
    akka ++ Seq(avro, ciris, refined, enumeratum) ++ cats ++ logging ++ joda ++ testDeps ++ kafkaClients ++ awsMskIamAuth ++ vulcan

  val avroDeps: Seq[ModuleID] =
    baseDeps ++ kafkaAvroSerializer ++ jackson ++ guavacache ++ catsEffect ++ redisCache

  val coreDeps: Seq[ModuleID] = akka ++ baseDeps ++
    Seq(
      reflections,
      retry
    ) ++ guavacache ++ kafkaAvroSerializer ++ kamon ++ redisCache

  val ingestDeps: Seq[ModuleID] = coreDeps ++ akkaHttpHal ++ Seq(embeddedKafka, sprayJson)

  val kafkaDeps: Seq[ModuleID] = coreDeps ++ Seq(
    akkaKafkaStream,
    refined,
    sprayJson
  ) ++ kafka ++ akkaHttpHal ++ fs2Kafka ++ integrationDeps ++ kafkaSchemaRegistry

  val awsAuthDeps: Seq[ModuleID] = awsSdk

  val kafkaSchemaRegistryDep = kafkaSchemaRegistry

}
