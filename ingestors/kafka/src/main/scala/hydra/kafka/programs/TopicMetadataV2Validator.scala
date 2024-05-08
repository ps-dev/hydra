package hydra.kafka.programs

import cats.effect.Sync
import cats.syntax.all._
import hydra.common.validation.{AdditionalValidation, AdditionalValidationUtil, MetadataAdditionalValidation, Validator}
import hydra.common.validation.Validator.ValidationChain
import hydra.kafka.algebras.MetadataAlgebra
import hydra.kafka.model.DataClassification._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model._

class TopicMetadataV2Validator[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F]) extends Validator {

  def validate(updateMetadataV2Request: TopicMetadataV2Request, subject: Subject): F[Unit] =
    for {
      metadata              <- metadataAlgebra.getMetadataFor(subject)
      _                     <- validateSubDataClassification(updateMetadataV2Request.dataClassification, updateMetadataV2Request.subDataClassification)
      _                     <- validateTopicsFormat(updateMetadataV2Request.replacementTopics)
      _                     <- validateTopicsFormat(updateMetadataV2Request.previousTopics)
      additionalValidations <- new AdditionalValidationUtil(
        isExistingTopic = metadata.isDefined,
        currentAdditionalValidations = metadata.flatMap(_.value.additionalValidations)
      ).pickValidations().getOrElse(List.empty).pure
      _                     <- validateAdditional(additionalValidations, updateMetadataV2Request, subject)
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

  private def validateTopicsFormat(maybeTopics: Option[List[String]]): F[Unit] = {
    val validationResults = for {
      topics <- maybeTopics
    } yield {
      topics map { topic =>
        val isValidTopicFormat = Subject.createValidated(topic).nonEmpty
        validate(isValidTopicFormat, TopicMetadataError.InvalidTopicFormatError(topic))
      }
    }
    resultOf(validationResults.getOrElse(List(Validator.valid)).pure)
  }

  private def validateAdditional(additionalValidations: List[AdditionalValidation],
                                 request: TopicMetadataV2Request,
                                 subject: Subject): F[Unit] =
    resultOf((additionalValidations collect {
      case m: MetadataAdditionalValidation => m
    } flatMap {
      case MetadataAdditionalValidation.replacementTopics =>
        List(validateDeprecatedTopicHasReplacementTopic(request.deprecated, request.replacementTopics, subject.value))
    }).pure)

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): ValidationChain = {
    val hasReplacementTopicsIfDeprecated = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    validate(hasReplacementTopicsIfDeprecated, TopicMetadataError.ReplacementTopicsMissingError(topic))
  }
}

object TopicMetadataV2Validator {
  def make[F[_] : Sync](metadataAlgebra: MetadataAlgebra[F]): TopicMetadataV2Validator[F] =
    new TopicMetadataV2Validator[F](metadataAlgebra)
}
