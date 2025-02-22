package hydra.kafka.model

import java.time.Instant
import java.util.UUID
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.syntax.all._
import cats.{Applicative, ApplicativeError, Monad, MonadError}
import fs2.kafka.Headers
import hydra.avro.convert.{ISODateConverter, IsoDate}
import hydra.common.validation.{AdditionalValidation, MetadataAdditionalValidation, SchemaAdditionalValidation}
import hydra.core.marshallers._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{Encoder, EncoderFactory}
import spray.json.DefaultJsonProtocol
import vulcan.generic._
import vulcan.refined._
import vulcan.{AvroError, AvroNamespace, Codec}

import scala.util.control.NoStackTrace

/**
  * Created by alexsilva on 3/30/17.
  */

case class TopicMetadata(
    subject: String,
    schemaId: Int,
    streamType: String,
    derived: Boolean,
    deprecated: Option[Boolean],
    replacementTopics: Option[List[String]],
    previousTopics: Option[List[String]],
    dataClassification: String,
    subDataClassification: Option[String],
    contact: String,
    additionalDocumentation: Option[String],
    notes: Option[String],
    id: UUID,
    createdDate: org.joda.time.DateTime,
    notificationUrl: Option[String],
    additionalValidationList: Option[List[AdditionalValidation]]
)

object TopicMetadataV2 {

  def getSchemas[F[_]: ApplicativeError[*[_], Throwable]]: F[Schemas] = {
    (
      Validated.fromEither(TopicMetadataV2Key.codec.schema).toValidatedNel,
      Validated.fromEither(TopicMetadataV2ValueOptionalTagList.codec.schema).toValidatedNel
    ).mapN(Schemas.apply) match {
      case Valid(s) =>
        Applicative[F].pure(s)
      case Invalid(e) =>
        ApplicativeError[F, Throwable].raiseError(MetadataAvroSchemaFailure(e))
    }
  }

  def encode[F[_]: MonadError[*[_], Throwable]](
      key: TopicMetadataV2Key,
      value: Option[TopicMetadataV2Value],
      headers: Option[Headers] = None
  ): F[(GenericRecord, Option[GenericRecord], Option[Headers])] = {
    Monad[F]
      .pure {
        val valueResult: Option[Either[AvroError, Any]] = value.map(a => TopicMetadataV2ValueOptionalTagList
          .codec.encode(a.toTopicMetadataV2ValueOptionalTagList))
        (
          Validated
            .fromEither(TopicMetadataV2Key.codec.encode(key))
            .toValidatedNel,
          valueResult match {
            case Some(e: Either[AvroError, Any]) =>
              Validated
                .fromEither(e)
                .toValidatedNel
            case None => None.validNel
          }
          ).tupled.toEither.leftMap { a =>
          MetadataAvroSchemaFailure(a)
        }
      }
      .rethrow
      .flatMap {
        case (k: GenericRecord, v: GenericRecord) => Monad[F].pure((k, Option(v), headers))
        case(k: GenericRecord, None)  => Monad[F].pure((k, None, headers))
        case (k, v) =>
          MonadError[F, Throwable].raiseError(
            AvroEncodingFailure(
              NonEmptyList.of(k, v, headers).map(_.getClass.getSimpleName)
            )
          )
      }
  }

  def decode[F[_]: MonadError[*[_], Throwable]](
                                               key: GenericRecord,
                                               value: Option[GenericRecord]
                                               ): F[(TopicMetadataV2Key, Option[TopicMetadataV2Value])] = {
    getSchemas[F].flatMap { schemas =>
      Monad[F]
        .pure {
          val valueResult: Option[Either[AvroError, TopicMetadataV2Value]] = value.map(TopicMetadataV2ValueOptionalTagList
            .codec.decode(_, schemas.value).map(_.toTopicMetadataV2Value))
          (
            Validated
              .fromEither(TopicMetadataV2Key.codec.decode(key, schemas.key))
              .toValidatedNel,
            valueResult match {
              case Some(Left(avroError)) =>
               avroError.invalidNel
              case Some (Right(topicMetadataV2Value)) =>
                Some(topicMetadataV2Value).validNel
              case None => None.validNel
            }
            ).tupled.toEither
            .leftMap { a =>
              MetadataAvroSchemaFailure(a)
            }
        }
        .rethrow
    }
  }

  final case class MetadataAvroSchemaFailure(errors: NonEmptyList[AvroError])
      extends NoStackTrace

  final case class AvroEncodingFailure(unexpectedTypes: NonEmptyList[String])
      extends NoStackTrace

}

@AvroNamespace("_hydra.v2")
final case class TopicMetadataV2Key(
    subject: Subject
)

object TopicMetadataV2Key {

  implicit val codec: Codec[TopicMetadataV2Key] =
    Codec.derive[TopicMetadataV2Key]

}

@AvroNamespace("_hydra.v2")
final case class TopicMetadataV2ValueOptionalTagList(
                                         streamType: StreamTypeV2,
                                         deprecated: Boolean,
                                         deprecatedDate: Option[Instant],
                                         replacementTopics: Option[List[String]],
                                         previousTopics: Option[List[String]],
                                         dataClassification: DataClassification,
                                         subDataClassification: Option[SubDataClassification],
                                         contact: NonEmptyList[ContactMethod],
                                         createdDate: Instant,
                                         parentSubjects: List[String],
                                         notes: Option[String],
                                         teamName: Option[String],
                                         tags: Option[List[String]],
                                         notificationUrl: Option[String],
                                         additionalValidations: Option[List[AdditionalValidation]]
                                       ) {
  def toTopicMetadataV2Value: TopicMetadataV2Value = {
    TopicMetadataV2Value(
      streamType,
      deprecated,
      deprecatedDate,
      replacementTopics,
      previousTopics,
      dataClassification,
      subDataClassification,
      contact,
      createdDate,
      parentSubjects,
      notes,
      teamName,
      tags.getOrElse(List.empty),
      notificationUrl,
      additionalValidations
    )
  }
}


final case class TopicMetadataV2Value(
    streamType: StreamTypeV2,
    deprecated: Boolean,
    deprecatedDate: Option[Instant],
    replacementTopics: Option[List[String]],
    previousTopics: Option[List[String]],
    dataClassification: DataClassification,
    subDataClassification: Option[SubDataClassification],
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[String],
    notes: Option[String],
    teamName: Option[String],
    tags: List[String],
    notificationUrl: Option[String],
    additionalValidations: Option[List[AdditionalValidation]]
) {
  def toTopicMetadataV2ValueOptionalTagList: TopicMetadataV2ValueOptionalTagList = {
    TopicMetadataV2ValueOptionalTagList(
      streamType,
      deprecated,
      deprecatedDate,
      replacementTopics,
      previousTopics,
      dataClassification,
      subDataClassification,
      contact,
      createdDate,
      parentSubjects,
      notes,
      teamName,
      tags.some,
      notificationUrl,
      additionalValidations
    )
  }
}

object TopicMetadataV2ValueOptionalTagList {

  implicit val streamTypeCodec: Codec[StreamTypeV2] =
    Codec.deriveEnum[StreamTypeV2](
      symbols = List("Event", "Entity", "Telemetry"),
      encode = {
        case StreamTypeV2.Event      => "Event"
        case StreamTypeV2.Entity     => "Entity"
        case StreamTypeV2.Telemetry  => "Telemetry"
      },
      decode = {
        case "Event"     => Right(StreamTypeV2.Event)
        case "Entity"    => Right(StreamTypeV2.Entity)
        case "Telemetry" => Right(StreamTypeV2.Telemetry)
        case other       => Left(AvroError(s"$other is not a StreamTypeV2"))
      }
    )

  implicit val dataClassificationCodec: Codec[DataClassification] =
    Codec.deriveEnum[DataClassification](
      symbols = List(
        DataClassification.Public.entryName,
        DataClassification.InternalUse.entryName,
        DataClassification.Confidential.entryName,
        DataClassification.Restricted.entryName,
        ObsoleteDataClassification.InternalUseOnly.entryName,
        ObsoleteDataClassification.ConfidentialPII.entryName,
        ObsoleteDataClassification.RestrictedEmployeeData.entryName,
        ObsoleteDataClassification.RestrictedFinancial.entryName
      ),
      encode = {
        case DataClassification.Public                         => DataClassification.Public.entryName
        case DataClassification.InternalUse                    => DataClassification.InternalUse.entryName
        case DataClassification.Confidential                   => DataClassification.Confidential.entryName
        case DataClassification.Restricted                     => DataClassification.Restricted.entryName
        case ObsoleteDataClassification.InternalUseOnly        => ObsoleteDataClassification.InternalUseOnly.entryName
        case ObsoleteDataClassification.ConfidentialPII        => ObsoleteDataClassification.ConfidentialPII.entryName
        case ObsoleteDataClassification.RestrictedEmployeeData => ObsoleteDataClassification.RestrictedEmployeeData.entryName
        case ObsoleteDataClassification.RestrictedFinancial    => ObsoleteDataClassification.RestrictedFinancial.entryName
      },
      decode = {
        case "Public"                 => Right(DataClassification.Public)
        case "InternalUse"            => Right(DataClassification.InternalUse)
        case "Confidential"           => Right(DataClassification.Confidential)
        case "Restricted"             => Right(DataClassification.Restricted)
        case "InternalUseOnly"        => Right(ObsoleteDataClassification.InternalUseOnly)
        case "ConfidentialPII"        => Right(ObsoleteDataClassification.ConfidentialPII)
        case "RestrictedEmployeeData" => Right(ObsoleteDataClassification.RestrictedEmployeeData)
        case "RestrictedFinancial"    => Right(ObsoleteDataClassification.RestrictedFinancial)
        case other                    => Left(AvroError(s"$other is not a DataClassification. Valid value is one of: ${DataClassification.values}"))
      }
    )

  implicit val subDataClassificationCodec: Codec[SubDataClassification] =
    Codec.deriveEnum[SubDataClassification](
      symbols = List(
        SubDataClassification.Public.entryName,
        SubDataClassification.InternalUseOnly.entryName,
        SubDataClassification.ConfidentialPII.entryName,
        SubDataClassification.RestrictedEmployeeData.entryName,
        SubDataClassification.RestrictedFinancial.entryName
      ),
      encode = {
        case SubDataClassification.Public                 => SubDataClassification.Public.entryName
        case SubDataClassification.InternalUseOnly        => SubDataClassification.InternalUseOnly.entryName
        case SubDataClassification.ConfidentialPII        => SubDataClassification.ConfidentialPII.entryName
        case SubDataClassification.RestrictedEmployeeData => SubDataClassification.RestrictedEmployeeData.entryName
        case SubDataClassification.RestrictedFinancial    => SubDataClassification.RestrictedFinancial.entryName
      },
      decode = {
        case "Public"                 => Right(SubDataClassification.Public)
        case "InternalUseOnly"        => Right(SubDataClassification.InternalUseOnly)
        case "ConfidentialPII"        => Right(SubDataClassification.ConfidentialPII)
        case "RestrictedEmployeeData" => Right(SubDataClassification.RestrictedEmployeeData)
        case "RestrictedFinancial"    => Right(SubDataClassification.RestrictedFinancial)
        case other                    => Left(AvroError(s"$other is not a SubDataClassification. Valid value is one of: ${SubDataClassification.values}"))
      }
    )

  private implicit val instantCodec: Codec[Instant] = Codec.string.imap { str =>
    new ISODateConverter()
      .fromCharSequence(
        str,
        org.apache.avro.SchemaBuilder.builder.stringType,
        IsoDate
      )
      .toInstant
  } { instant => instant.toString }

  private implicit val contactMethodCodec: Codec[ContactMethod] =
    Codec.derive[ContactMethod]

  private implicit val additionalValidationCodec: Codec[AdditionalValidation] = Codec.deriveEnum[AdditionalValidation](
    symbols = List(
      MetadataAdditionalValidation.replacementTopics.entryName,
      MetadataAdditionalValidation.contact.entryName,
      SchemaAdditionalValidation.defaultInRequiredField.entryName,
      SchemaAdditionalValidation.timestampMillis.entryName
    ),
    encode = {
      case MetadataAdditionalValidation.replacementTopics    => MetadataAdditionalValidation.replacementTopics.entryName
      case MetadataAdditionalValidation.contact              => MetadataAdditionalValidation.contact.entryName
      case SchemaAdditionalValidation.defaultInRequiredField => SchemaAdditionalValidation.defaultInRequiredField.entryName
      case SchemaAdditionalValidation.timestampMillis        => SchemaAdditionalValidation.timestampMillis.entryName
    },
    decode = {
      case "replacementTopics"      => Right(MetadataAdditionalValidation.replacementTopics)
      case "contact"                => Right(MetadataAdditionalValidation.contact)
      case "defaultInRequiredField" => Right(SchemaAdditionalValidation.defaultInRequiredField)
      case "timestampMillis"        => Right(SchemaAdditionalValidation.timestampMillis)
      case other                    => Left(AvroError(s"$other is not a ${AdditionalValidation.toString}"))
    }
  )

  implicit val codec: Codec[TopicMetadataV2ValueOptionalTagList] =
  Codec.record[TopicMetadataV2ValueOptionalTagList](
    name = "TopicMetadataV2Value",
    namespace = "_hydra.v2"
  ) {
    field =>
      (field("streamType", _.streamType),
        field("deprecated", _.deprecated),
        field("deprecatedDate", _.deprecatedDate, default = Some(None)),
        field("replacementTopics", _.replacementTopics, default = Some(None)),
        field("previousTopics", _.previousTopics, default = Some(None)),
        field("dataClassification", _.dataClassification),
        field("subDataClassification", _.subDataClassification, default = Some(None)),
        field("contact", _.contact),
        field("createdDate", _.createdDate),
        field("parentSubjects", _.parentSubjects),
        field("notes", _.notes, default = Some(None)),
        field("teamName", _.teamName, default = Some(None)),
        field("tags", _.tags, default = Some(None)),
        field("notificationUrl", _.notificationUrl, default = Some(None)),
        field("additionalValidations", _.additionalValidations, default = Some(None))
        ).mapN(TopicMetadataV2ValueOptionalTagList.apply)
  }
}
