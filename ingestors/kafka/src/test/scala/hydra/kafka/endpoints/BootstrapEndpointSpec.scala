package hydra.kafka.endpoints

import cats.effect.IO
import akka.actor.{Actor, ActorRef, Props}
import akka.http.javadsl.server.MalformedRequestContentRejection
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RequestEntityExpectedRejection
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestKit
import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.KafkaConfigUtils.SchemaRegistrySecurityConfig
import hydra.common.config.{ConfigSupport, KafkaConfigUtils}
import hydra.core.http.CorsSupport
import hydra.core.http.security.entity.AwsConfig
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.core.protocol.{Ingest, IngestorCompleted, IngestorError}
import hydra.kafka.marshallers.HydraKafkaJsonSupport
import hydra.kafka.model.{DataClassification, SubDataClassification, TopicMetadata}
import hydra.kafka.producer.AvroRecord
import hydra.kafka.services.StreamsManagerActor
import hydra.kafka.util.{KafkaUtils, MetadataUtils}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json._

import scala.concurrent.duration._
import scala.io.Source

class BootstrapEndpointSpec
    extends Matchers
    with AnyWordSpecLike
    with ScalatestRouteTest
    with HydraKafkaJsonSupport
    with ConfigSupport
    with EmbeddedKafka
    with MockFactory
    with Eventually {

  private implicit val timeout = RouteTestTimeout(10.seconds)

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 8012,
    zooKeeperPort = 3011,
    customBrokerProperties = Map("auto.create.topics.enable" -> "false")
  )

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(5000 millis), interval = scaled(100 millis))

  private implicit val corsSupport: CorsSupport = new CorsSupport("*")

  class TestKafkaIngestor extends Actor {

    override def receive = {
      case Ingest(hydraRecord, _)
          if hydraRecord
            .asInstanceOf[AvroRecord]
            .payload
            .get("subject") == "exp.dataplatform.failed" =>
        sender ! IngestorError(new Exception("oh noes!"))
      case Ingest(_, _) => sender ! IngestorCompleted
    }

    def props: Props = Props()
  }

  class IngestorRegistry extends Actor {
    context.actorOf(Props(new TestKafkaIngestor), "kafka_ingestor")

    override def receive: Receive = {
      case _ =>
    }
  }

  val ingestorRegistry =
    system.actorOf(Props(new IngestorRegistry), "ingestor_registry")

  private[kafka] val bootstrapKafkaConfig =
    applicationConfig.getConfig("bootstrap-config")

  private val schemaRegistryEmptySecurityConfig = SchemaRegistrySecurityConfig(None, None)

  private val awsSecurityService = mock[AwsSecurityService[IO]]
  private val auth = new AccessControlService[IO](awsSecurityService, AwsConfig(Some("somecluster"), isAwsIamSecurityEnabled = false))

  val streamsManagerProps = StreamsManagerActor.props(
    bootstrapKafkaConfig,
    KafkaConfigUtils.kafkaSecurityEmptyConfig,
    KafkaUtils.BootstrapServers,
    ConfluentSchemaRegistry.forConfig(applicationConfig, schemaRegistryEmptySecurityConfig).registryClient
  )
  val streamsManagerActor: ActorRef = system.actorOf(streamsManagerProps, "streamsManagerActor")

  private val bootstrapRoute = new BootstrapEndpoint(system, streamsManagerActor, KafkaConfigUtils.kafkaSecurityEmptyConfig, schemaRegistryEmptySecurityConfig, auth, awsSecurityService).route

  implicit val f = jsonFormat16(TopicMetadata)

  private val topicMetadataJson = Source.fromResource("HydraMetadataTopic.avsc").mkString

  override def beforeAll: Unit = {
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("_hydra.metadata.topic")
    EmbeddedKafka.createCustomTopic("dvs.test.existing")
    EmbeddedKafka.createCustomTopic("dvs.test.v2.existing")
  }

  override def afterAll = {
    super.afterAll()
    EmbeddedKafka.stop()
    TestKit.shutdownActorSystem(
      system,
      verifySystemShutdown = true,
      duration = 10.seconds
    )
  }

  def v1CreateTopicRequest(namespace: String = "exp.assessment",
                           name: String = "SkillAssessmentTopicsScored",
                           deprecated: Boolean = false,
                           replacementTopics: Option[List[String]] = None,
                           previousTopics: Option[List[String]] = None,
                           dataClassification: String = "InternalUse",
                           subDataClassification: Option[String] = None,
                           contact: String = "#slackity-slack-dont-talk-back"): HttpEntity.Strict = HttpEntity(
    ContentTypes.`application/json`,
    s"""{
       |	"streamType": "Notification",
       |  "derived": false,
       |  "deprecated": $deprecated,
       |	"dataClassification": "$dataClassification",
       |  ${if (replacementTopics.nonEmpty) s""""replacementTopics": ${replacementTopics.toJson},""" else ""}
       |  ${if (previousTopics.nonEmpty) s""""previousTopics": ${previousTopics.toJson},""" else ""}
       |	${if (subDataClassification.isDefined) s""""subDataClassification": "${subDataClassification.get}",""" else ""}
       |	"dataSourceOwner": "BARTON",
       |	"contact": "$contact",
       |	"psDataLake": false,
       |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
       |	"notes": "here are some notes topkek",
       |	"schema": {
       |	  "namespace": "$namespace",
       |	  "name": "$name",
       |	  "type": "record",
       |	  "version": 1,
       |	  "fields": [
       |	    {
       |	      "name": "testField",
       |	      "type": "string"
       |	    }
       |	  ]
       |	},
       | "notificationUrl": "notification.url"
       |}""".stripMargin
  )

  def dataClassificationRequest(dataClassification: String, subDataClassification: Option[String] = None): HttpEntity.Strict =
    v1CreateTopicRequest(dataClassification = dataClassification, subDataClassification = subDataClassification)

  "The bootstrap endpoint" should {
    "list streams" in {

      val json =
        s"""{
           |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
           | "createdDate":"${ISODateTimeFormat
             .basicDateTimeNoMillis()
             .print(DateTime.now)}",
           | "subject": "exp.assessment.SkillAssessmentTopicsScored",
           |	"streamType": "Notification",
           | "derived": false,
           |	"dataClassification": "Public",
           |	"subDataClassification": "Public",
           |	"contact": "#slackity-slack-dont-talk-back",
           |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
           |	"notes": "here are some notes topkek",
           |	"schemaId": 2,
           |  "notificationUrl": "notification.url"
           |}""".stripMargin.parseJson
          .convertTo[TopicMetadata]

      val schema = new Schema.Parser().parse(topicMetadataJson)

      val record: Object = new JsonConverter[GenericRecord](schema)
        .convert(json.toJson.compactPrint)
      implicit val deserializer = new KafkaAvroSerializer(
        ConfluentSchemaRegistry.forConfig(applicationConfig, schemaRegistryEmptySecurityConfig).registryClient
      )
      EmbeddedKafka.publishToKafka("_hydra.metadata.topic", record)

      eventually {
        Get("/streams") ~> bootstrapRoute ~> check {
          val r = responseAs[Seq[TopicMetadata]]
          r.length should be >= 1
          r.head.id.toString shouldBe "79a1627e-04a6-11e9-8eb2-f2801f1b9fd1"
        }
      }
    }

    "get a stream by subject" in {

      val json =
        s"""{
           |	"id":"79a1627e-04a6-11e9-8eb2-f2801f1b9fd1",
           | "createdDate":"${ISODateTimeFormat
             .basicDateTimeNoMillis()
             .print(DateTime.now)}",
           | "subject": "exp.assessment.SkillAssessmentTopicsScored1",
           |	"streamType": "Notification",
           | "derived": false,
           |	"dataClassification": "Public",
           |	"subDataClassification": "Public",
           |	"contact": "#slackity-slack-dont-talk-back",
           |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
           |	"notes": "here are some notes topkek",
           |	"schemaId": 2,
           |  "notificationUrl": "notification.url"
           |}""".stripMargin.parseJson
          .convertTo[TopicMetadata]

      val schema = new Schema.Parser().parse(topicMetadataJson)

      val record: Object = new JsonConverter[GenericRecord](schema)
        .convert(json.toJson.compactPrint)
      implicit val deserializer = new KafkaAvroSerializer(
        ConfluentSchemaRegistry.forConfig(applicationConfig, schemaRegistryEmptySecurityConfig).registryClient
      )
      EmbeddedKafka.publishToKafka("_hydra.metadata.topic", record)

      eventually {
        Get("/streams/exp.assessment.SkillAssessmentTopicsScored1") ~> bootstrapRoute ~> check {
          val r = responseAs[Seq[TopicMetadata]]
          r.length should be >= 1
          r(0).id.toString shouldBe "79a1627e-04a6-11e9-8eb2-f2801f1b9fd1"
        }
      }
    }

    "reject empty requests" in {
      Post("/streams") ~> bootstrapRoute ~> check {
        rejection shouldEqual RequestEntityExpectedRejection
      }
    }

    "complete all 3 steps (ingest metadata, register schema, create topic) for valid requests" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp.assessment",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val r = responseAs[TopicMetadata]
        r.streamType shouldBe "Notification"
      }
    }

    "return the correct response when the ingestor fails" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp.dataplatform",
          |	  "name": "failed",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject requests with invalid topic names" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "namespace": "exp",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject requests with invalid generic schema" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject invalid metadata payloads" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamName": "invalid",
          |	"streamType": "Historical",
          |	"dataSourceOwner": "BARTON",
          |	"dataSourceContact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"dataDocPath": "akka://some/path/here.jpggifyo",
          |	"dataOwnerNotes": "here are some notes topkek",
          |	"streamSchema": {
          |	  "namespace": "exp.assessment",
          |	  "name": "SkillAssessmentTopicsScored",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      Post("/streams", testEntity) ~> bootstrapRoute ~> check {
        rejection shouldBe a[MalformedRequestContentRejection]
      }
    }

    "accept previously existing invalid schema" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |   "namespace": "exp.test-existing.v1",
          |	  "name": "SubjectPreexisted",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      val bootstrapRouteWithOverridenStreamManager =
        (new BootstrapEndpoint(system, streamsManagerActor, KafkaConfigUtils.kafkaSecurityEmptyConfig, schemaRegistryEmptySecurityConfig, auth, awsSecurityService) with BootstrapEndpointTestActors[IO]).route
      Post("/streams", testEntity) ~> bootstrapRouteWithOverridenStreamManager ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "verify that creation date does not change on updates" in {
      val testEntity = HttpEntity(
        ContentTypes.`application/json`,
        """{
          |	"streamType": "Notification",
          | "derived": false,
          |	"dataClassification": "Public",
          |	"subDataClassification": "Public",
          |	"dataSourceOwner": "BARTON",
          |	"contact": "#slackity-slack-dont-talk-back",
          |	"psDataLake": false,
          |	"additionalDocumentation": "akka://some/path/here.jpggifyo",
          |	"notes": "here are some notes topkek",
          |	"schema": {
          |   "namespace": "exp.test-existing.v1",
          |	  "name": "SubjectPreexisted",
          |	  "type": "record",
          |	  "version": 1,
          |	  "fields": [
          |	    {
          |	      "name": "testField",
          |	      "type": "string"
          |	    }
          |	  ]
          |	},
          | "notificationUrl": "notification.url"
          |}""".stripMargin
      )

      val bootstrapRouteWithOverridenStreamManager =
        (new BootstrapEndpoint(system, streamsManagerActor, KafkaConfigUtils.kafkaSecurityEmptyConfig, schemaRegistryEmptySecurityConfig, auth, awsSecurityService) with BootstrapEndpointTestActors[IO]).route
      Get("/streams/exp.test-existing.v1.SubjectPreexisted") ~> bootstrapRouteWithOverridenStreamManager ~> check {
        val originalTopicData = responseAs[Seq[TopicMetadata]]
        val originalTopicCreationDate = originalTopicData.head.createdDate
        Post("/streams", testEntity) ~> bootstrapRouteWithOverridenStreamManager ~> check {
          status shouldBe StatusCodes.OK
          val response2 = responseAs[TopicMetadata]
          response2.createdDate shouldBe originalTopicCreationDate
        }
      }
    }

    DataClassification.values foreach { dc =>
      s"$dc: accept valid as well as obsolete(enforced by backward schema evolution) DataClassification value" in {
        Post("/streams", dataClassificationRequest(dc.entryName)) ~> bootstrapRoute ~> check {
          status shouldBe StatusCodes.OK
        }
      }

      if (dc == DataClassification.Restricted) {
        s"$dc: accept the request when SubDataClassification value cannot be derived from DataClassification" in {
          Post("/streams", dataClassificationRequest(dc.entryName)) ~> bootstrapRoute ~> check {
            status shouldBe StatusCodes.OK
            val response = responseAs[TopicMetadata]
            response.dataClassification shouldBe MetadataUtils.oldToNewDataClassification(dc.entryName)
            response.subDataClassification shouldBe None
          }
        }

        s"$dc: accept the request when SubDataClassification value cannot be derived from DataClassification honoring the user given SDC value" in {
          Post("/streams", dataClassificationRequest(dc.entryName, Some("RestrictedEmployeeData"))) ~> bootstrapRoute ~> check {
            status shouldBe StatusCodes.OK
            val response = responseAs[TopicMetadata]
            response.dataClassification shouldBe MetadataUtils.oldToNewDataClassification(dc.entryName)
            response.subDataClassification shouldBe Some("RestrictedEmployeeData")
          }
        }

        s"$dc: validate the user given SubDataClassification value when it cannot be derived from DataClassification" in {
          Post("/streams", dataClassificationRequest(dc.entryName, Some("junk"))) ~> bootstrapRoute ~> check {
            status shouldBe StatusCodes.BadRequest
            responseAs[String] shouldBe "[\"junk is not a valid symbol. Possible values are: [Public, InternalUseOnly, ConfidentialPII, RestrictedFinancial, RestrictedEmployeeData].\"]"
          }
        }
      } else {
        s"$dc: accept the request when SubDataClassification value can be derived from DataClassification ignoring user given SDC value" in {
          Post("/streams", dataClassificationRequest(dc.entryName, Some("junk"))) ~> bootstrapRoute ~> check {
            status shouldBe StatusCodes.OK
            val response = responseAs[TopicMetadata]
            response.dataClassification shouldBe MetadataUtils.oldToNewDataClassification(dc.entryName)
            response.subDataClassification shouldBe MetadataUtils.deriveSubDataClassification(dc.entryName)
          }
        }
      }
    }

    SubDataClassification.values foreach { value =>
      s"$value: accept valid SubDataClassification values" in {
        Post("/streams", dataClassificationRequest(value.entryName, Some(value.entryName))) ~> bootstrapRoute ~> check {
          status shouldBe StatusCodes.OK
        }
      }
    }

    "reject invalid DataClassification metadata value" in {
      Post("/streams", dataClassificationRequest("junk")) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "reject invalid SubDataClassification metadata value when its value cannot be derived from DataClassification" in {
      Post("/streams",
        dataClassificationRequest(
          dataClassification = "Restricted",
          subDataClassification = Some("junk"))) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "v1: reject a request when a topic being deprecated does not have replacementTopics" in {
      val namespace = "exp.dvs"
      val name = "v1.test"
      Post("/streams",
        v1CreateTopicRequest(namespace, name, deprecated = true)
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe s"""["Field 'replacementTopics' is required when the topic '$namespace.$name' is being deprecated!"]"""
      }
    }

    "v1: reject a request when a topic being deprecated contains replacementTopics with all non-existing topics" in {
      Post("/streams",
        v1CreateTopicRequest(deprecated = true, replacementTopics = Some(List("dvs.test.not.existing", "invalid.dvs.testing")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe
          s"""
             |[
             |"Topic 'dvs.test.not.existing' does not exist!",
             |"Topic 'invalid.dvs.testing' does not exist!"
             |]
             |""".stripMargin.replace("\n", "")
      }
    }

    "v1: reject a request when a topic being deprecated contains replacementTopics with some non-existing topics" in {
      Post("/streams",
        v1CreateTopicRequest(deprecated = true, replacementTopics = Some(List("dvs.test.existing", "invalid.dvs.testing")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe
          s"""
             |[
             |"Topic 'invalid.dvs.testing' does not exist!"
             |]
             |""".stripMargin.replace("\n", "")
      }
    }

    "v1: reject a request when a new topic being created is deprecated pointing to itself in replacementTopics" in {
      val namespace = "exp.dvs"
      val name = "v1.test"
      Post("/streams",
        v1CreateTopicRequest(namespace, name, deprecated = true, replacementTopics = Some(List(s"$namespace.$name")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe s"""["Topic '$namespace.$name' does not exist!"]"""
      }
    }

    "v1: accept a request when an existing topic being deprecated points to itself in replacementTopics" in {
      val namespace = "dvs.test"
      val name = "existing"
      val existingTopic = s"$namespace.$name"

      Post("/streams",
        v1CreateTopicRequest(namespace, name, deprecated = true, replacementTopics = Some(List(existingTopic)))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[String].parseJson
        validateResponseString(response, namespace, name, deprecated = true, replacementTopics = List(existingTopic))
      }
    }

    "v1: accept a request when a topic being deprecated contains replacementTopics with all existing topics" in {
      val replacementTopics = List("dvs.test.existing", "dvs.test.v2.existing")
      Post("/streams",
        v1CreateTopicRequest(deprecated = true, replacementTopics = Some(replacementTopics))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[String].parseJson
        validateResponseString(response, deprecated = true, replacementTopics = replacementTopics)
      }
    }

    "v1: accept a request when a topic NOT being deprecated contains replacementTopics with all existing topics" in {
      val replacementTopics = List("dvs.test.existing", "dvs.test.v2.existing")
      Post("/streams",
        v1CreateTopicRequest(replacementTopics = Some(replacementTopics))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[String].parseJson
        validateResponseString(response, replacementTopics = replacementTopics)
      }
    }

    "v1: reject a request when a topic NOT being deprecated points to itself in replacementTopics" in {
      val replacementTopics = List("dvs.test.existing", "dvs.test.v2.existing")
      Post("/streams",
        v1CreateTopicRequest(namespace = "dvs.test", name = "existing", replacementTopics = Some(replacementTopics))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe
          s"""
             |[
             |"A non-deprecated topic 'dvs.test.existing' pointing to itself in replacementTopics is not useful!"
             |]
             |""".stripMargin.replace("\n", "")
      }
    }

    "v1: reject a request when previousTopics contains all non-existing topics" in {
      Post("/streams",
        v1CreateTopicRequest(previousTopics = Some(List("dvs.test.not.existing", "invalid.dvs.testing")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe
          s"""
             |[
             |"Topic 'dvs.test.not.existing' does not exist!",
             |"Topic 'invalid.dvs.testing' does not exist!"
             |]
             |""".stripMargin.replace("\n", "")
      }
    }

    "v1: reject a request when previousTopics contains some non-existing topics" in {
      Post("/streams",
        v1CreateTopicRequest(previousTopics = Some(List("dvs.test.existing", "dvs.test.not.existing")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe
          s"""
             |[
             |"Topic 'dvs.test.not.existing' does not exist!"
             |]
             |""".stripMargin.replace("\n", "")
      }
    }

    "v1: reject a request when previousTopics contains itself" in {
      val namespace = "dvs.test"
      val name = "existing"
      Post("/streams",
        v1CreateTopicRequest(namespace, name, previousTopics = Some(List(s"$namespace.$name")))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe s"""["Previous topics cannot point to itself, '$namespace.$name'!"]"""
      }
    }

    "v1: accept a request when all topics in previousTopics exist" in {
      val previousTopics = List("dvs.test.existing", "dvs.test.v2.existing")
      Post("/streams",
        v1CreateTopicRequest(previousTopics = Some(previousTopics))
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.OK
        val response = responseAs[String].parseJson
        validateResponseString(response, previousTopics = previousTopics)
      }
    }

    "v1: reject a request when slackChannel in contact is not valid for a new topic" in {
      val slackChannel = "slackity-slack-dont-talk-back"
      Post("/streams",
        v1CreateTopicRequest(contact = slackChannel)
      ) ~> bootstrapRoute ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe s"""["Field `contact` must be a Slack channel starting with '#', all lowercase, with no spaces, and less than 80 characters"]"""
      }
    }
  }

  def validateResponseString(response: JsValue,
                             namespace: String = "exp.assessment",
                             name: String = "SkillAssessmentTopicsScored",
                             deprecated: Boolean = false,
                             replacementTopics: List[String] = Nil,
                             previousTopics: List[String] = Nil): Assertion = {
    val fields = response.asJsObject.fields
    val topicName = s"$namespace.$name"
    val id = fields.getOrElse("id", "")
    val schemaId = fields.getOrElse("schemaId", "")
    val createdDate = fields.getOrElse("createdDate", "")
    response shouldBe
      s"""{
         |  "_links": {
         |    "hydra-schema": {
         |      "href": "/schemas/$topicName"
         |    },
         |    "self": {
         |      "href": "/streams/$topicName"
         |    }
         |  },
         |  "additionalDocumentation": "akka://some/path/here.jpggifyo",
         |  "contact": "#slackity-slack-dont-talk-back",
         |  "createdDate": $createdDate,
         |  "dataClassification": "InternalUse",
         |  "deprecated": $deprecated,
         |  ${if (replacementTopics.nonEmpty) s""""replacementTopics": ${replacementTopics.toJson},\n""" else ""}
         |  ${if (previousTopics.nonEmpty) s""""previousTopics": ${previousTopics.toJson},\n""" else ""}
         |  "derived": false,
         |  "id": $id,
         |  "notes": "here are some notes topkek",
         |  "notificationUrl": "notification.url",
         |  "schemaId": $schemaId,
         |  "streamType": "Notification",
         |  "subDataClassification": "InternalUseOnly",
         |  "subject": "$topicName"
         |}""".stripMargin.parseJson
  }
}
