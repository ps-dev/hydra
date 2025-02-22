package hydra.kafka.services

import java.net.InetAddress
import java.util.UUID
import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control

import scala.jdk.CollectionConverters._
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.pattern.pipe
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import com.typesafe.config.Config
import hydra.common.config.ConfigSupport
import hydra.common.config.ConfigSupport._
import hydra.common.config.KafkaConfigUtils._
import hydra.common.validation.{AdditionalValidation, MetadataAdditionalValidation, SchemaAdditionalValidation}
import hydra.core.marshallers.HydraJsonSupport
import hydra.kafka.model.TopicMetadata
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.format.ISODateTimeFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class StreamsManagerActor(
    bootstrapKafkaConfig: Config,
    kafkaClientSecurityConfig: KafkaClientSecurityConfig,
    bootstrapServers: String,
    schemaRegistryClient: SchemaRegistryClient
) extends Actor
    with ConfigSupport
    with HydraJsonSupport
    with ActorLogging {

  import StreamsManagerActor._

  private implicit val ec = context.dispatcher

  private implicit val system = context.system

  private val metadataTopicName = bootstrapKafkaConfig
    .getStringOpt("metadata-topic-name")
    .getOrElse("_hydra.metadata.topic")

  private[kafka] val metadataMap = Map[String, TopicMetadata]()

  private val metadataStream = StreamsManagerActor.createMetadataStream(
    bootstrapKafkaConfig,
    kafkaClientSecurityConfig,
    bootstrapServers,
    schemaRegistryClient,
    metadataTopicName,
    self
  )

  override def receive: Receive = Actor.emptyBehavior

  override def preStart(): Unit = {
    context.become(streaming(metadataStream.run(), metadataMap))
  }

  def streaming(
      stream: (Control, NotUsed),
      metadataMap: Map[String, TopicMetadata]
  ): Receive = {
    case InitializedStream =>
      sender ! MetadataProcessed

    case GetMetadata =>
      sender ! GetMetadataResponse(metadataMap)

    case t: TopicMetadata =>
      context.become(streaming(stream, metadataMap + (t.subject -> t)))
      sender ! MetadataProcessed

    case t: TopicMetadataMessage =>
      val newMap = t.topicMetadata match {
        case Some(value) => metadataMap + (t.subject -> value)
        case None => metadataMap - t.subject
      }

      context.become(streaming(stream, newMap))
      sender ! MetadataProcessed

    case StopStream =>
      pipe(stream._1.shutdown().map(_ => StreamStopped)) to sender

    case GetStreamActor(actorName: String) =>
      pipe(Future {
        GetStreamActorResponse(context.child(actorName))
      }) to sender

    case StreamFailed(ex) =>
      log.error("StreamsManagerActor stream failed with exception", ex)
  }
}

object StreamsManagerActor {

  private type Stream = RunnableGraph[(Control, NotUsed)]

  case object GetMetadata

  case object MetadataProcessed

  case class GetStreamActor(actorName: String)

  case class GetMetadataResponse(metadata: Map[String, TopicMetadata])

  case class GetStreamActorResponse(actor: Option[ActorRef])

  case object StopStream

  case object StreamStopped

  case class StreamFailed(ex: Throwable)

  case object InitializedStream

  case class TopicMetadataMessage(subject: String, topicMetadata: Option[TopicMetadata])

  def getMetadataTopicName(c: Config) =
    c.getStringOpt("metadata-topic-name")
      .getOrElse("_hydra.metadata.topic")

  private def toOptionList(record: GenericRecord, fieldName: String): Option[List[String]] =
    if (record.hasField(fieldName) && record.get(fieldName) != null) {
      Option(record.get(fieldName).toString) map { value =>
        val trimmed = value.trim.stripPrefix("[").stripSuffix("]")
        trimmed.split(",").map(_.trim).toList
      }
    } else {
      None
    }

  private def toAdditionalValidationList(record: GenericRecord): Option[List[AdditionalValidation]] =
    toOptionList(record, "additionalValidationList").map { stringList =>
      stringList.flatMap {
        case "replacementTopics" => Some(MetadataAdditionalValidation.replacementTopics)
        case "contact" => Some(MetadataAdditionalValidation.contact)
        case "defaultInRequiredField" => Some(SchemaAdditionalValidation.defaultInRequiredField)
        case "timestampMillis" => Some(SchemaAdditionalValidation.timestampMillis)
        case _ => None
      }
    }

  private[services] def createMetadataStream[K, V](
      config: Config,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig,
      bootstrapSevers: String,
      schemaRegistryClient: SchemaRegistryClient,
      metadataTopicName: String,
      destination: ActorRef
  )(implicit ec: ExecutionContext, s: ActorSystem): Stream = {

    val formatter = ISODateTimeFormat.basicDateTimeNoMillis()

    val maybeHost = Try {
      InetAddress.getLocalHost.getHostName.split("\\.")(0)
    }.getOrElse(UUID.randomUUID().toString)

    val settings = ConsumerSettings(
      config,
      new StringDeserializer,
      new KafkaAvroDeserializer(schemaRegistryClient)
    ).withBootstrapServers(bootstrapSevers)
      .withGroupId(s"metadata-consumer-actor-$maybeHost")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withKafkaSecurityConfigs(kafkaClientSecurityConfig)

    Consumer
      .plainSource(settings, Subscriptions.topics(metadataTopicName))
      .map { msg =>
        val topicMetadata = Option(msg.value).map { value =>
          val record = value.asInstanceOf[GenericRecord]
          TopicMetadata(
            record.get("subject").toString,
            record.get("schemaId").toString.toInt,
            record.get("streamType").toString,
            record.get("derived").toString.toBoolean,
            Try(Option(record.get("deprecated"))).toOption.flatten.map(_.toString.toBoolean),
            toOptionList(record, "replacementTopics"),
            toOptionList(record, "previousTopics"),
            record.get("dataClassification").toString,
            Try(Option(record.get("subDataClassification"))).toOption.flatten.map(_.toString),
            record.get("contact").toString,
            Try(Option(record.get("additionalDocumentation"))).toOption.flatten.map(_.toString),
            Try(Option(record.get("notes"))).toOption.flatten.map(_.toString),
            UUID.fromString(record.get("id").toString),
            formatter.parseDateTime(record.get("createdDate").toString),
            Try(Option(record.get("notificationUrl"))).toOption.flatten.map(_.toString),
            toAdditionalValidationList(record)
          )
        }
        TopicMetadataMessage(msg.key, topicMetadata)
      }
      .toMat(
        Sink.actorRefWithBackpressure(
          destination,
          onInitMessage = InitializedStream,
          ackMessage = MetadataProcessed,
          onCompleteMessage = StreamStopped,
          onFailureMessage = StreamFailed.apply
        )
      )(Keep.both)
  }

  def props(
      bootstrapKafkaConfig: Config,
      kafkaClientSecurityConfig: KafkaClientSecurityConfig,
      bootstrapServers: String,
      schemaRegistryClient: SchemaRegistryClient
  ): Props = {
    Props(new StreamsManagerActor(bootstrapKafkaConfig, kafkaClientSecurityConfig, bootstrapServers, schemaRegistryClient))
  }
}
