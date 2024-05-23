package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.{AdditionalValidation, AdditionalValidationUtil, MetadataAdditionalValidation, Validator}
import hydra.common.validation.Validator.{ValidationChain, valid}
import hydra.kafka.algebras.{KafkaAdminAlgebra, MetadataAlgebra}
import hydra.kafka.model.DataClassification._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._

class TopicMetadataV2Validator[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F], kafkaAdmin: KafkaAdminAlgebra[F]) extends Validator {

  def validate(updateMetadataV2Request: TopicMetadataV2Request, subject: Subject): F[Unit] =
    for {
      metadata              <- metadataAlgebra.getMetadataFor(subject)
      _                     <- validateSubDataClassification(updateMetadataV2Request.dataClassification, updateMetadataV2Request.subDataClassification)
      _                     <- validateTopicsExist(updateMetadataV2Request.replacementTopics)
      _                     <- validateTopicsExist(updateMetadataV2Request.previousTopics)
      _                     <- validatePreviousTopicsCannotPointItself(updateMetadataV2Request.previousTopics, subject.value)
      additionalValidations <- new AdditionalValidationUtil(
        isExistingTopic = metadata.isDefined,
        currentAdditionalValidations = metadata.flatMap(_.value.additionalValidations)
      ).pickValidations().getOrElse(List.empty).pure
      _                     <- validateAdditional(additionalValidations, updateMetadataV2Request, subject.value)
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

  private def validateAdditional(additionalValidations: List[AdditionalValidation],
                                 request: TopicMetadataV2Request,
                                 topic: String): F[Unit] = {
    val validations = additionalValidations.collect {
      case MetadataAdditionalValidation.replacementTopics =>
        validateDeprecatedTopicHasReplacementTopic(request.deprecated, request.replacementTopics, topic)
    }
    resultOf(validations.pure)
  }

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): ValidationChain = {
    val hasReplacementTopicsIfDeprecated = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    validate(hasReplacementTopicsIfDeprecated, TopicMetadataError.ReplacementTopicsMissingError(topic))
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
