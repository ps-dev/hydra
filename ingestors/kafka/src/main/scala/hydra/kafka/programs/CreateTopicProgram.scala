package hydra.kafka.programs
import java.time.Instant
import cats.effect.{Bracket, ExitCase, Resource, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.SchemaVersion
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.{SkipValidation, StreamTypeV2, TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Request}
import hydra.kafka.programs.CreateTopicProgram._
import hydra.kafka.util.KafkaUtils.TopicDetails
import org.typelevel.log4cats.Logger
import org.apache.avro.Schema
import retry.syntax.all._
import retry._
import cats.implicits._
import hydra.common.validation.AdditionalValidationUtil
import hydra.kafka.model.TopicMetadataV2Request.Subject

import scala.language.higherKinds
import scala.util.control.NoStackTrace

final class CreateTopicProgram[F[_]: Bracket[*[_], Throwable]: Sleep: Logger] private (
                                                                               schemaRegistry: SchemaRegistry[F],
                                                                               kafkaAdmin: KafkaAdminAlgebra[F],
                                                                               kafkaClient: KafkaClientAlgebra[F],
                                                                               retryPolicy: RetryPolicy[F],
                                                                               v2MetadataTopicName: Subject,
                                                                               metadataAlgebra: MetadataAlgebra[F],
                                                                               schemaValidator: KeyAndValueSchemaV2Validator[F],
                                                                               metadataValidator: TopicMetadataV2Validator[F]
                                                                             ) (implicit eff: Sync[F]){

  private def onFailure(resourceTried: String): (Throwable, RetryDetails) => F[Unit] = {
    (error, retryDetails) =>
      Logger[F].info(
        s"Retrying due to failure in $resourceTried: $error. RetryDetails: $retryDetails"
      )
  }

  private def registerSchema(
                              subject: Subject,
                              schema: Schema,
                              isKey: Boolean
                            ): Resource[F, Unit] = {
    val suffixedSubject = subject.value + (if (isKey) "-key" else "-value")
    val registerSchema: F[Option[SchemaVersion]] = {
      schemaRegistry
        .getVersion(suffixedSubject, schema)
        .attempt
        .map(_.toOption)
        .flatMap { previousSchemaVersion =>
          schemaRegistry.registerSchema(suffixedSubject, schema) *>
            schemaRegistry.getVersion(suffixedSubject, schema).map {
              newSchemaVersion =>
                if (previousSchemaVersion.contains(newSchemaVersion)) {
                  None
                }
                else {
                  Some(newSchemaVersion)
                }
            }
        }
    }.retryingOnAllErrors(retryPolicy, onFailure("RegisterSchema"))
    Resource
      .makeCase(registerSchema)((newVersionMaybe, exitCase) =>
        (exitCase, newVersionMaybe) match {
          case (ExitCase.Error(_), Some(newVersion)) =>
            schemaRegistry.deleteSchemaOfVersion(suffixedSubject, newVersion)
          case _ => Bracket[F, Throwable].unit
        }
      )
      .void
  }

  private[programs] def registerSchemas(
                                         subject: Subject,
                                         keySchema: Schema,
                                         valueSchema: Schema
                                       ): Resource[F, Unit] = {
    registerSchema(subject, keySchema, isKey = true) *> registerSchema(
      subject,
      valueSchema,
      isKey = false
    )
  }

  private[programs] def createTopicResource(
                                             subject: Subject,
                                             topicDetails: TopicDetails
                                           ): Resource[F, Unit] = {
    val createTopic: F[Option[Subject]] =
      kafkaAdmin.describeTopic(subject.value).flatMap {
        case Some(_) => Bracket[F, Throwable].pure(None)
        case None =>
          kafkaAdmin
            .createTopic(subject.value, topicDetails)
            .retryingOnAllErrors(retryPolicy, onFailure("CreateTopicResource")) *>
            Bracket[F, Throwable].pure(Some(subject))
      }
    Resource
      .makeCase(createTopic)({
        case (Some(_), ExitCase.Error(_)) =>
          kafkaAdmin.deleteTopic(subject.value)
        case _ => Bracket[F, Throwable].unit
      })
      .void
  }

  private def publishMetadata(
                               topicName: Subject,
                               createTopicRequest: TopicMetadataV2Request,
                             ): F[Unit] = {
    for {
      metadata <- metadataAlgebra.getMetadataFor(topicName)
      createdDate = metadata.map(_.value.createdDate).getOrElse(createTopicRequest.createdDate)
      deprecatedDate = metadata.map(_.value.deprecatedDate).getOrElse(createTopicRequest.deprecatedDate) match {
        case Some(date) =>
          Some(date)
        case None =>
          if(createTopicRequest.deprecated) {
            Some(Instant.now)
          } else {
            None
          }
      }
      additionalValidationUtility = new AdditionalValidationUtil(
        isExistingTopic = metadata.isDefined,
        currentAdditionalValidations = metadata.flatMap(_.value.additionalValidations))
      message = (TopicMetadataV2Key(topicName),
        createTopicRequest.copy(
          createdDate = createdDate,
          deprecatedDate = deprecatedDate,
          additionalValidations = additionalValidationUtility.pickValidations()
        ).toValue)
      records <- TopicMetadataV2.encode[F](message._1, Some(message._2), None)
      _ <- kafkaClient
        .publishMessage(records, v2MetadataTopicName.value)
        .rethrow
    } yield ()
  }

  private def checkThatTopicExists(topicName: String): F[Unit] =
    for {
      result <- kafkaAdmin.describeTopic(topicName)
      _ <- eff.fromOption(result, MetadataOnlyTopicDoesNotExist(topicName))
    } yield ()

  //todo: workaround for https://pluralsight.atlassian.net/browse/ADAPT-929, should be removed in the future
  def createTopicFromMetadataOnly(topicName: Subject, createTopicRequest: TopicMetadataV2Request, withRequiredFields: Boolean = false,
                                  maybeSkipValidations: Option[List[SkipValidation]] = None): F[Unit] =
    for {
      _ <- checkThatTopicExists(topicName.value)
      _ <- schemaValidator.validate(createTopicRequest, topicName, withRequiredFields, maybeSkipValidations)
      _ <- metadataValidator.validate(createTopicRequest, topicName)
      _ <- publishMetadata(topicName, createTopicRequest)
    } yield ()

  //todo: workaround for https://pluralsight.atlassian.net/browse/ADAPT-929, should be removed in the future
  def createTopic(
                   topicName: Subject,
                   createTopicRequest: TopicMetadataV2Request,
                   defaultTopicDetails: TopicDetails,
                   withRequiredFields: Boolean = false
                 ): F[Unit] = {
    def getCleanupPolicyConfig: Map[String, String] =
      createTopicRequest.streamType match {
        case StreamTypeV2.Entity => Map("cleanup.policy" -> "compact")
        case _ => Map.empty
      }

    val td = createTopicRequest.numPartitions.fold(defaultTopicDetails)(numP =>
      defaultTopicDetails.copy(numPartitions = numP.value))
      .copy(partialConfig = defaultTopicDetails.configs ++ getCleanupPolicyConfig)

    (for {
      _ <- Resource.eval(schemaValidator.validate(createTopicRequest, topicName, withRequiredFields))
      _ <- Resource.eval(metadataValidator.validate(createTopicRequest, topicName))
      _ <- registerSchemas(
        topicName,
        createTopicRequest.schemas.key,
        createTopicRequest.schemas.value
      )
      _ <- createTopicResource(topicName, td)
      _ <- Resource.eval(publishMetadata(topicName, createTopicRequest))
    } yield ()).use(_ => Bracket[F, Throwable].unit)
  }
}

object CreateTopicProgram {
  def make[F[_]: Bracket[*[_], Throwable]: Sleep: Logger](
                                                           schemaRegistry: SchemaRegistry[F],
                                                           kafkaAdmin: KafkaAdminAlgebra[F],
                                                           kafkaClient: KafkaClientAlgebra[F],
                                                           retryPolicy: RetryPolicy[F],
                                                           v2MetadataTopicName: Subject,
                                                           metadataAlgebra: MetadataAlgebra[F]
                                                         ) (implicit eff: Sync[F]): CreateTopicProgram[F] = {
    new CreateTopicProgram(
      schemaRegistry,
      kafkaAdmin,
      kafkaClient,
      retryPolicy,
      v2MetadataTopicName,
      metadataAlgebra,
      KeyAndValueSchemaV2Validator.make(schemaRegistry, metadataAlgebra),
      TopicMetadataV2Validator.make(metadataAlgebra, kafkaAdmin)
    )
  }

  final case class MetadataOnlyTopicDoesNotExist(topicName: String) extends NoStackTrace {
    override def getMessage: String = s"You cannot add metadata for topic '$topicName' if it does not exist in the cluster. Please create your topic first."
  }
}
