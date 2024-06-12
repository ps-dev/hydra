package hydra.kafka.services

import hydra.common.validation.MetadataAdditionalValidation
import hydra.core.marshallers.{GenericSchema, TopicMetadataRequest}
import hydra.kafka.programs.TopicMetadataError
import hydra.kafka.util.{KafkaUtils, MetadataUtils}
import hydra.kafka.util.MetadataUtils

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

  def validate(request: TopicMetadataRequest, schema: GenericSchema, kafkaUtils: KafkaUtils): Try[ValidationResponse] =
    mergeValidationResponses(
      List(
        validateSubject(Option(schema)),
        validateTopicsExist(request.replacementTopics, kafkaUtils),
        validateTopicsExist(request.previousTopics, kafkaUtils),
        validateDeprecatedTopicHasReplacementTopic(request.deprecated.contains(true), request.replacementTopics, schema.subject),
        validateNonDepSelfRefReplacementTopics(request.deprecated.contains(true), request.replacementTopics, schema.subject),
        validatePreviousTopicsCannotPointItself(request.previousTopics, schema.subject),
        validateAdditional(request)
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

  private def validateAdditional(request: TopicMetadataRequest): Try[ValidationResponse] = {
    val invalidReasons = request.additionalValidations.getOrElse(Nil).collect {
      case MetadataAdditionalValidation.contact =>
        validContact(request.contact)
    }.collect {
      case Invalid(reason) => reason
    }

    if (invalidReasons.nonEmpty) {
      Failure(ValidatorException(invalidReasons))
    }
    else {
      Success(Valid)
    }
  }

  private def validContact(contactField: String): ValidationResponse = {
    if (contactField.matches("""^#[a-z][a-z_-]{0,78}$""")) {
      Valid
    } else {
      Invalid(InvalidContactProvided)
    }
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

  private def validateDeprecatedTopicHasReplacementTopic(deprecated: Boolean,
                                                         replacementTopics: Option[List[String]],
                                                         topic: String): Try[ValidationResponse] = {
    val isDeprecatedWithReplacementTopic = if (deprecated) replacementTopics.exists(_.nonEmpty) else true
    if (isDeprecatedWithReplacementTopic) {
      Success(Valid)
    } else {
      val errorMessage = TopicMetadataError.ReplacementTopicsMissingError(topic).message
      Failure(ValidatorException(Seq(errorMessage)))
    }
  }

  private def validateNonDepSelfRefReplacementTopics(deprecated: Boolean,
                                                     replacementTopics: Option[List[String]],
                                                     topic: String): Try[ValidationResponse] = {
    val topicNotDeprecatedDoesNotPointToSelf = if (!deprecated) replacementTopics.forall(!_.contains(topic)) else true
    if (topicNotDeprecatedDoesNotPointToSelf) {
      Success(Valid)
    } else {
      val errorMessage = TopicMetadataError.SelfRefReplacementTopicsError(topic).message
      Failure(ValidatorException(Seq(errorMessage)))
    }
  }

  private def mergeValidationResponses(validationList: List[Try[ValidationResponse]]): Try[ValidationResponse] =
    validationList collect {
      case Failure(ValidatorException(failures)) => failures
    } match {
      case failures: Seq[Seq[String]] if failures.nonEmpty => Failure(ValidatorException(failures.flatten))
      case _ => Success(Valid)
    }

  private def validateTopicsExist(maybeTopics: Option[List[String]], kafkaUtils: KafkaUtils): Try[ValidationResponse] =
    maybeTopics.map(doTopicsExist(_, kafkaUtils)).getOrElse(Success(Valid))

  private def validatePreviousTopicsCannotPointItself(maybeTopics: Option[List[String]], currentTopic: String): Try[ValidationResponse] =
    maybeTopics match {
      case Some(topics) if topics.contains(currentTopic) => Failure(ValidatorException(List(s"Previous topics cannot point to itself, '$currentTopic'!")))
      case _                                             => Success(Valid)
    }

  private def doTopicsExist(topics: List[String], kafkaUtils: KafkaUtils): Try[ValidationResponse] = {
    val notExistingTopics = topics.collect {
      case topic if !kafkaUtils.topicExists(topic).get => s"Topic '$topic' does not exist!"
    }

    if (notExistingTopics.nonEmpty) {
      Failure(ValidatorException(notExistingTopics))
    }
    else {
      Success(Valid)
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

  val InvalidContactProvided: String =
    "Field `contact` must be a Slack channel starting with '#', all lowercase, with no spaces, and less than 80 characters"
}

sealed trait ValidationResponse

case object Valid extends ValidationResponse

case class Invalid(reason: String) extends ValidationResponse

case class ValidatorException(reasons: Seq[String])
    extends RuntimeException
