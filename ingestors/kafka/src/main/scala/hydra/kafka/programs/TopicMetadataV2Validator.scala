package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.Validator.valid
import hydra.common.validation.{AdditionalValidation, AdditionalValidationUtil, MetadataAdditionalValidation, Validator}
import hydra.kafka.algebras.{KafkaAdminAlgebra, MetadataAlgebra}
import hydra.kafka.model.ContactMethod.Slack
import hydra.kafka.model.DataClassification._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._

class TopicMetadataV2Validator[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F], kafkaAdmin: KafkaAdminAlgebra[F]) extends Validator {

  def validate(request: TopicMetadataV2Request, subject: Subject): F[Unit] =
    for {
      metadata              <- metadataAlgebra.getMetadataFor(subject)
      _                     <- validateSubDataClassification(request.dataClassification, request.subDataClassification)
      _                     <- validateTopicsExist(request.replacementTopics)
      _                     <- validateTopicsExist(request.previousTopics)
      _                     <- validateDeprecatedTopicHasReplacementTopic(request.deprecated, request.replacementTopics, subject.value)
      _                     <- validateNonDepSelfRefReplacementTopics(request.deprecated, request.replacementTopics, subject.value)
      _                     <- validatePreviousTopicsCannotPointItself(request.previousTopics, subject.value)
      additionalValidations <- new AdditionalValidationUtil(
        isExistingTopic = metadata.isDefined,
        currentAdditionalValidations = metadata.flatMap(_.value.additionalValidations)
      ).pickValidations().getOrElse(List.empty).pure
      _                     <- validateAdditional(additionalValidations, request)
    } yield ()

  private def validateSubDataClassification(dataClassification: DataClassification,
                                            subDataClassification: Option[SubDataClassification]): F[Unit] = {
    (subDataClassification map { sdc =>
      val correctionRequired = collectValidSubDataClassificationValues(dataClassification, sdc)
      resultOf(validate(
        correctionRequired.isEmpty,
        TopicMetadataError.InvalidSubDataClassificationTypeError(dataClassification.entryName, sdc.entryName, correctionRequired)
      ))
    }).getOrElse(().pure)
  }

  private def collectValidSubDataClassificationValues(dc: DataClassification, sdc: SubDataClassification): Seq[SubDataClassification] = dc match {
    case Public       => if (sdc != SubDataClassification.Public) Seq(SubDataClassification.Public) else Seq.empty
    case InternalUse  => if (sdc != SubDataClassification.InternalUseOnly) Seq(SubDataClassification.InternalUseOnly) else Seq.empty
    case Confidential => if (sdc != SubDataClassification.ConfidentialPII) Seq(SubDataClassification.ConfidentialPII) else Seq.empty
    case Restricted   =>
      if (sdc != SubDataClassification.RestrictedFinancial && sdc != SubDataClassification.RestrictedEmployeeData) {
        Seq(SubDataClassification.RestrictedFinancial, SubDataClassification.RestrictedEmployeeData)
      } else {
        Seq.empty
      }
  }

  private def validateTopicsExist(maybeTopics: Option[List[String]]): F[Unit] = {
    val validationChains = for {
      topics <- maybeTopics.getOrElse(List.empty).traverse(checkIfTopicExists)
      (_, notExistingTopics) = topics.partition(_._2)
    } yield {
      notExistingTopics collect {
        case (topic, isExisting) => validate(isExisting, TopicMetadataError.TopicDoesNotExist(topic))
      }
    }

    resultOf(validationChains)
  }

  private def checkIfTopicExists(topicName: String): F[(String, Boolean)] =
    kafkaAdmin.describeTopic(topicName).map { t =>
      (topicName, t.isDefined)
    }

  private def validateAdditional(additionalValidations: List[AdditionalValidation], request: TopicMetadataV2Request): F[Unit] = {
    val maybeSlackChannel = extractSlackChannel(request)
    val validations = additionalValidations.collect {
      // Add extra validations applicable on topics created after replacementTopics feature was introduced.
      case MetadataAdditionalValidation.replacementTopics => valid
      case MetadataAdditionalValidation.contactValidation if maybeSlackChannel.isDefined =>
        val slackChannel = maybeSlackChannel.get
        val slackChannelValidation = slackChannel.matches("""^#[a-z][a-z_-]{0,78}$""")
        validate(slackChannelValidation, TopicMetadataError.InvalidContactProvided(slackChannel))
    }
    resultOf(validations.pure)
  }

  private def extractSlackChannel(request: TopicMetadataV2Request): Option[String] = {
    request.contact.collect {
      case Slack(channel) => channel.value
    }.headOption
  }

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): F[Unit] = {
    val hasReplacementTopicsIfDeprecated = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    resultOf(validate(hasReplacementTopicsIfDeprecated, TopicMetadataError.ReplacementTopicsMissingError(topic)))
  }

  private def validateNonDepSelfRefReplacementTopics(deprecated: Boolean,
                                                     replacementTopics: Option[List[String]],
                                                     topic: String): F[Unit] = {
    val topicNotDeprecatedDoesNotPointToSelf = if (!deprecated) replacementTopics.forall(!_.contains(topic)) else true
    resultOf(validate(topicNotDeprecatedDoesNotPointToSelf, TopicMetadataError.SelfRefReplacementTopicsError(topic)))
  }

  private def validatePreviousTopicsCannotPointItself(maybeTopics: Option[List[String]], currentTopic: String): F[Unit] = {
    val validationChain = maybeTopics match {
      case Some(topics) => validate(!topics.contains(currentTopic), TopicMetadataError.PreviousTopicsCannotPointItself(currentTopic))
      case None         => valid
    }

    resultOf(validationChain)
  }
}

object TopicMetadataV2Validator {
  def make[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F], kafkaAdmin: KafkaAdminAlgebra[F]): TopicMetadataV2Validator[F] =
    new TopicMetadataV2Validator[F](metadataAlgebra, kafkaAdmin)
}
