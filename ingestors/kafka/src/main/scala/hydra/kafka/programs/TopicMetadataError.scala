package hydra.kafka.programs

import hydra.common.validation.ValidationError
import hydra.kafka.model.SubDataClassification
import hydra.kafka.model.TopicMetadataV2Request.Subject

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

  case class InvalidTopicFormatError(topic: String) extends TopicMetadataError {
    override def message: String = s"$topic : ${Subject.invalidFormat}"
  }

  case class TopicDoesNotExist(topic: String) extends TopicMetadataError {
    override def message: String = s"Topic '$topic' does not exist!"
  }

  case class PreviousTopicsCannotPointItself(topic: String) extends TopicMetadataError {
    override def message: String = s"Previous topics cannot point to itself, '$topic'!"
  }
}
