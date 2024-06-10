package hydra.kafka.services

import hydra.core.marshallers.GenericSchema
import hydra.kafka.services.ErrorMessages._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.{Failure, Success}

class TopicMetadataValidatorSpec extends AnyFlatSpecLike with Matchers {

  "A TopicNameValidator" should "return Valid for a valid topic" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "dvs.test_topic.v1"))
    ) shouldBe Success(Valid)
  }

  it should "return Invalid for a topic name longer than 249 characters" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "exp.test." + "a" * 250))
    ) shouldBe Failure(ValidatorException(Seq(LengthError)))
  }

  it should "return Invalid for a topic containing invalid characters" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "exp.test.Test(Topic)"))
    ) shouldBe Failure(ValidatorException(Seq(InvalidCharacterError)))
  }

  it should "return invalid if doesn't start with a valid org prefix" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "false.test.TestTopic"))
    ) shouldBe Failure(ValidatorException(Seq(BadOrgError)))
  }

  it should "be properly formatted by containing at least 3 segments" in {
    TopicMetadataValidator.validateSubject(Some(GenericSchema("TestEntity", "exp"))) shouldBe Failure(
      ValidatorException(Seq(BadTopicFormatError))
    )
  }

  it should "return multiple errors if validation fails for multiple reasons" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "falsetestTestTopic"))
    ) shouldBe Failure(
      ValidatorException(Seq(BadOrgError, BadTopicFormatError))
    )
  }

  it should "return no errors if lgl is used as the organization" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("TestEntity", "lgl.test.test_topic.v1"))
    ) shouldBe Success(Valid)
  }

  it should "return Invalid for namespace or name containing hyphens" in {
    TopicMetadataValidator.validateSubject(
      Some(GenericSchema("Test-Entity", "exp.test.l-1325"))
    ) should not be Failure(
      ValidatorException(Seq(InvalidCharacterError))
    )
  }

  it should "return Invalid for missing generic schema" in {
    TopicMetadataValidator.validateSubject(None) shouldBe Failure(
      ValidatorException(Seq(SchemaError))
    )
  }

}
