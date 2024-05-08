package hydra.kafka.services

import hydra.common.validation.MetadataAdditionalValidation
import hydra.core.marshallers.{GenericSchema, TopicMetadataRequest}
import hydra.kafka.programs.TopicMetadataError

import scala.util.{Failure, Success, Try}

object TopicMetadataValidator {

  import ErrorMessages._

  private val subjectValidationFunctions: Seq[String => ValidationResponse] =
    List(
      topicIsTooLong,
      topicContainsInvalidChars,
      validOrg,
      validFormat
    )

  def validate(metadataRequest: TopicMetadataRequest, schema: GenericSchema): Try[ValidationResponse] =
    mergeValidationResponses(
      List(
        validateSubject(Option(schema)),
        validateMetadata(metadataRequest, schema.subject),
        validateTopics(metadataRequest.replacementTopics),
        validateTopics(metadataRequest.previousTopics)
      )
    )

  def validateSubject(gOpt: Option[GenericSchema]): Try[ValidationResponse] = {
    gOpt match {
      case Some(g) => validateSubject(s"${g.namespace}.${g.name}")
      case None    => Failure(ValidatorException(SchemaError :: Nil))
    }
  }

  def validateSubject(subject: String): Try[ValidationResponse] =
    subjectValidationFunctions
      .map(f => f(subject))
      .collect {
        case r: Invalid => r
      } match {
      case respSeq: Seq[ValidationResponse] if respSeq.nonEmpty =>
        Failure(
          ValidatorException(respSeq.map(invalid => invalid.reason))
        )
      case _ => scala.util.Success(Valid)
    }

  private def topicIsTooLong(topic: String): ValidationResponse = {
    topic match {
      case s: String if s.length <= 249 => Valid
      case _                            => Invalid(LengthError)
    }
  }

  private def topicContainsInvalidChars(topic: String): ValidationResponse = {
    if (topic.matches("""^[a-zA-Z0-9._\-]*$""")) {
      Valid
    } else {
      Invalid(InvalidCharacterError)
    }
  }

  private def validOrg(topic: String): ValidationResponse = {
    val validOrgs = Set("exp", "rev", "fin", "mkg", "pnp", "sbo", "dvs", "lgl")

    val orgOpt = topic.split("\\.").headOption

    orgOpt match {
      case Some(org) =>
        if (validOrgs contains org) {
          Valid
        } else {
          Invalid(BadOrgError)
        }
      case None => Invalid(BadOrgError)
    }
  }

  private def validFormat(topic: String): ValidationResponse = {
    topic.split("\\.").filterNot(_ == "").toList match {
      case Nil                             => Invalid(BadTopicFormatError)
      case l: List[String] if l.length < 3 => Invalid(BadTopicFormatError)
      case _                               => Valid
    }
  }

  private def validateMetadata(metadataRequest: TopicMetadataRequest, topic: String): Try[ValidationResponse] =
    validateAdditional(metadataRequest, topic) collect {
      case i: Invalid => i
    } match {
      case respSeq: Seq[ValidationResponse] if respSeq.nonEmpty => Failure(ValidatorException(respSeq.map(_.reason)))
      case _ => scala.util.Success(Valid)
    }

  private def validateAdditional(metadataRequest: TopicMetadataRequest, topic: String): List[ValidationResponse] = {
    val metadataValidations = metadataRequest.additionalValidations map { av =>
      av collect {
        case m: MetadataAdditionalValidation => m
      }
    }

    metadataValidations.getOrElse(List()) map {
      case MetadataAdditionalValidation.replacementTopics =>
        validateDeprecatedTopicHasReplacementTopic(metadataRequest.deprecated.contains(true), metadataRequest.replacementTopics, topic)
    }
  }

  def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean, replacementTopics: Option[List[String]], topic: String): ValidationResponse = {
    val isDeprecatedWithReplacementTopic = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    if (isDeprecatedWithReplacementTopic) {
      Valid
    } else {
      Invalid(TopicMetadataError.ReplacementTopicsMissingError(topic).message)
    }
  }

  private def mergeValidationResponses(validationList: List[Try[ValidationResponse]]): Try[ValidationResponse] =
    validationList collect {
      case Failure(ValidatorException(failures)) => failures
    } match {
      case failures: Seq[Seq[String]] if failures.nonEmpty => Failure(ValidatorException(failures.flatten))
      case _ => Success(Valid)
    }

  private def validateTopics(replacementTopics: Option[List[String]]): Try[ValidationResponse] = {
    replacementTopics match {
      case Some(rtList) => mergeValidationResponses(rtList.map(x => validateSubject(x)))
      case None         => Success(Valid)
    }
  }
}

object ErrorMessages {
  val SchemaError: String = "Schema does not contain a namespace and a name"

  val LengthError: String =
    "Schema namespace +  schema name longer than 249 characters"

  val InvalidCharacterError: String =
    "Schema namespace + schema name may only contain letters, numbers and the characters '.'," +
      " '_'" + " '-'"

  val BadOrgError: String =
    "Schema namespace must begin with one of the following organizations: exp | rev | fin |" +
      " mkg | pnp | sbo | dvs"

  val BadTopicFormatError: String =
    "Schema must be formatted as <organization>.<product|context|" +
      "team>[.<version>].<entity> where <entity> is the name and the rest is the namespace of the schema"
}

sealed trait ValidationResponse

case object Valid extends ValidationResponse

case class Invalid(reason: String) extends ValidationResponse

case class ValidatorException(reasons: Seq[String])
    extends RuntimeException
