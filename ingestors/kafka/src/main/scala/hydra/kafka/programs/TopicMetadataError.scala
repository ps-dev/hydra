package hydra.kafka.programs

import hydra.common.validation.ValidationError
import hydra.kafka.model.SubDataClassification

sealed trait TopicMetadataError extends ValidationError

object TopicMetadataError {

  case class InvalidSubDataClassificationTypeError(dataClassification: String,
                                                   subDataClassification: String,
                                                   supportedValues: Seq[SubDataClassification]) extends TopicMetadataError {

    override def message: String = {
      val validValues = s"Valid value is ${if (supportedValues.size == 1) s"'${supportedValues.head}'" else s"one of [${supportedValues.mkString(", ")}]"}."
      s"'$subDataClassification' is not a valid SubDataClassification value for '$dataClassification' DataClassification. $validValues"
    }
  }

  case class ReplacementTopicsMissingError(topic: String) extends TopicMetadataError {
    override def message: String = s"Field 'replacementTopics' is required when the topic '$topic' is being deprecated!"
  }

  case class SelfRefReplacementTopicsError(topic: String) extends TopicMetadataError {
    override def message: String = s"A non-deprecated topic '$topic' pointing to itself in replacementTopics is not useful!"
  }

  case class TopicDoesNotExist(topic: String) extends TopicMetadataError {
    override def message: String = s"Topic '$topic' does not exist!"
  }

  case class PreviousTopicsCannotPointItself(topic: String) extends TopicMetadataError {
    override def message: String = s"Previous topics cannot point to itself, '$topic'!"
  }

  case class InvalidContactProvided(contactField: String) extends TopicMetadataError {
    override def message: String = s"Field `slackChannel` must start with '#' and must be all lowercase with no spaces and less than 80 characters, received'$contactField'!"
  }
}
