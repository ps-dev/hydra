package hydra.common.validation

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

@AvroNamespace("hydra.kafka.model")
sealed trait AdditionalValidation extends EnumEntry
sealed trait MetadataAdditionalValidation extends AdditionalValidation
sealed trait SchemaAdditionalValidation extends AdditionalValidation

// NOTE: Please note that any case object added here once must be retained throughout for schema to evolve.
object MetadataAdditionalValidation extends Enum[MetadataAdditionalValidation] {

  case object replacementTopics extends MetadataAdditionalValidation
  case object contactValidation extends MetadataAdditionalValidation

  override val values: immutable.IndexedSeq[MetadataAdditionalValidation] = findValues
}

// NOTE: Please note that any value added here once must be retained throughout for schema to evolve.
object SchemaAdditionalValidation extends Enum[SchemaAdditionalValidation] {

  case object defaultInRequiredField extends SchemaAdditionalValidation
  case object timestampMillis extends SchemaAdditionalValidation

  override val values: immutable.IndexedSeq[SchemaAdditionalValidation] = findValues
}

object AdditionalValidation {

  lazy val allValidations: Option[List[AdditionalValidation]] =
    Some(MetadataAdditionalValidation.values.toList ++ SchemaAdditionalValidation.values.toList)
}

class AdditionalValidationUtil(isExistingTopic: Boolean, currentAdditionalValidations: Option[List[AdditionalValidation]]) {

  def pickValidations(): Option[List[AdditionalValidation]] =
    if (isExistingTopic) currentAdditionalValidations else AdditionalValidation.allValidations

  def isPresent(additionalValidation: AdditionalValidation): Boolean =
    pickValidations().exists(_.contains(additionalValidation))
}