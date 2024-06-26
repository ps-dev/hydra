package hydra.kafka.serializers

import java.time.Instant
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import cats.data.Validated.{Invalid, Valid}
import cats.data._
import cats.syntax.all._
import enumeratum.EnumEntry
import eu.timepit.refined.auto._
import hydra.common.serdes.EnumEntryJsonFormat
import hydra.common.validation.AdditionalValidation
import hydra.kafka.model.ContactMethod.{Email, Slack}
import hydra.kafka.model.TopicMetadataV2Request.{NumPartitions, Subject}
import hydra.kafka.model._
import hydra.kafka.serializers.Errors._
import hydra.kafka.serializers.TopicMetadataV2Parser.IntentionallyUnimplemented
import hydra.kafka.util.MetadataUtils
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import spray.json.JsonFormat
import spray.json.JsNumber

object TopicMetadataV2Parser extends TopicMetadataV2Parser {
  case object IntentionallyUnimplemented extends RuntimeException
}

sealed trait TopicMetadataV2Parser
    extends SprayJsonSupport
    with DefaultJsonProtocol
    with TopicMetadataV2Validator {

  implicit object SubjectFormat extends RootJsonFormat[Subject] {

    override def write(obj: Subject): JsValue = {
      JsString(obj.value)
    }

    override def read(json: JsValue): Subject = json match {
      case JsString(value) =>
        Subject
          .createValidated(value)
          .getOrElse(
            throw DeserializationException(
              Subject.invalidFormat
            )
          )
      case j => throw DeserializationException(Subject.invalidFormat)
    }
  }

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = Instant.now()
  }

  implicit object ContactFormat
      extends RootJsonFormat[NonEmptyList[ContactMethod]] {

    def extractContactMethod[A](
        optionalField: Option[JsValue],
        f: String => Option[A],
        e: JsValue => String
    ): Either[String, Option[A]] = {
      optionalField match {
        case Some(JsString(value)) =>
          f(value) match {
            case Some(fval) => Right(Some(fval))
            case _          => Left(e(JsString(value)))
          }
        case _ =>
          Right(None)
      }
    }

    def separateEithers(
        email: Either[String, Option[Email]],
        slackChannel: Either[String, Option[Slack]]
    ): ValidatedNel[String, List[ContactMethod]] = {
      val e = Validated.fromEither(email).toValidatedNel
      val s = Validated.fromEither(slackChannel).toValidatedNel
      (e, s).mapN {
        case (Some(em), sl)   => List(em) ++ sl
        case (None, Some(sl)) => List(sl)
        case _                => List.empty
      }
    }

    def read(json: JsValue): NonEmptyList[ContactMethod] = {
      val deserializationExceptionMessage =
        ContactMissingContactOption.errorMessage
      json match {
        case JsObject(fields)
            if fields.isDefinedAt("email") || fields.isDefinedAt(
              "slackChannel"
            ) =>
          val email = extractContactMethod(
            fields.get("email"),
            Email.create,
            Errors.invalidEmailProvided
          )

          val slackChannel = extractContactMethod(
            fields.get("slackChannel"),
            Slack.create,
            Errors.invalidSlackChannelProvided
          )
          separateEithers(email, slackChannel) match {
            case Valid(contactMethods) =>
              NonEmptyList
                .fromList(contactMethods)
                .getOrElse(
                  throw DeserializationException(
                    deserializationExceptionMessage
                  )
                )
            case Invalid(e) =>
              val errorString = e.toList.mkString(" ")
              throw DeserializationException(errorString)
          }
        case _ =>
          throw DeserializationException(deserializationExceptionMessage)
      }
    }

    def write(obj: NonEmptyList[ContactMethod]): JsValue = {
      val map = obj
        .map {
          case Email(address) =>
            ("email", JsString(address))
          case Slack(channel) =>
            ("slackChannel", JsString(channel))
          case _ => ("unspecified", JsString("unspecified"))
        }
        .toList
        .toMap
      JsObject(map)
    }
  }

  implicit object StreamTypeV2Format
    extends RootJsonFormat[StreamTypeV2] {

    def read(json: JsValue): StreamTypeV2 = json match {
      case JsString("Entity")                 => StreamTypeV2.Entity
      case JsString("Event")                  => StreamTypeV2.Event
      case JsString("Telemetry")              => StreamTypeV2.Telemetry
      case _ =>
        import scala.reflect.runtime.{universe => ru}
        val tpe = ru.typeOf[StreamTypeV2]
        val knownDirectSubclasses: Set[ru.Symbol] =
          tpe.typeSymbol.asClass.knownDirectSubclasses
        throw DeserializationException(
          StreamTypeInvalid(json, knownDirectSubclasses).errorMessage
        )
    }

    def write(obj: StreamTypeV2): JsValue = {
      JsString(obj.toString)
    }
  }

  implicit val dataClassificationFormat: EnumEntryJsonFormat[DataClassification] =
    new EnumEntryJsonFormat[DataClassification](DataClassification.values)

  implicit val subDataClassificationFormat: EnumEntryJsonFormat[SubDataClassification] =
    new EnumEntryJsonFormat[SubDataClassification](SubDataClassification.values)

  implicit object SchemasFormat extends RootJsonFormat[Schemas] {

    override def write(obj: Schemas): JsValue = {
      JsObject(
        Map[String, JsValue](
          "key" -> new SchemaFormat(isKey = true).write(obj.key),
          "value" -> new SchemaFormat(isKey = false).write(obj.value)
        )
      )
    }

    override def read(json: JsValue): Schemas = json match {
      case j: JsObject =>
        val key = Try(
          new SchemaFormat(isKey = true)
            .read(j.getFields("key").headOption.getOrElse(JsObject.empty))
        )
        val value = Try(
          new SchemaFormat(isKey = false)
            .read(j.getFields("value").headOption.getOrElse(JsObject.empty))
        )
        (key, value) match {
          case (Success(keySchema), Success(valueSchema)) =>
            Schemas(keySchema, valueSchema)
          case _ =>
            val errorMessage = List(key, value)
              .filter(_.isFailure)
              .flatMap {
                case Success(_)         => None
                case Failure(exception) => Some(exception.getMessage)
              }
              .mkString(" ")
            throw DeserializationException(errorMessage)
        }
      case j => throw DeserializationException(InvalidSchemas(j).errorMessage)
    }
  }

  class SchemaFormat(isKey: Boolean) extends RootJsonFormat[Schema] {

    override def write(obj: Schema): JsValue = {
      import spray.json._
      obj.toString().parseJson
    }

    def isNamespaceInvalid(schema: Schema): Boolean = {
      val t = schema.getType
      t match {
        case Schema.Type.RECORD =>
          val currentNamespace = Option(schema.getNamespace).exists(f => !f.matches("^[A-Za-z0-9_\\.]*"))
          val allRecords = schema.getFields.asScala.toList.exists(f => isNamespaceInvalid(f.schema()))
          currentNamespace || allRecords
        case _ => false
      }
    }

    override def read(json: JsValue): Schema = {
      val jsonString = json.compactPrint
      val schema = try {
        new Schema.Parser().parse(jsonString)
      } catch {
        case e: Throwable => throw DeserializationException(InvalidSchema(json, isKey, Some(e)).errorMessage)
      }

      if(isNamespaceInvalid(schema)) {
        throw DeserializationException(InvalidSchema(json, isKey,
          Some(InvalidNamespace("Invalid character. Namespace must conform to regex ^[A-Za-z0-9_\\.]*"))).errorMessage)
      } else {
        schema
      }
    }
  }

  implicit val topicMetadataNumPartitionsFormat: JsonFormat[TopicMetadataV2Request.NumPartitions] = new RootJsonFormat[TopicMetadataV2Request.NumPartitions] {
    def read(json: JsValue): TopicMetadataV2Request.NumPartitions = {
      val int = json.convertTo[Int]
      TopicMetadataV2Request.NumPartitions.from(int) match {
        case Right(value) => value
        case Left(value) => throw new DeserializationException(value)
      }
    }
    
    def write(obj: TopicMetadataV2Request.NumPartitions): JsValue = JsNumber(obj.value)
    
  }

  class ListEnumEntryJsonFormat[E <: List[EnumEntry]](values: E) extends RootJsonFormat[E] {

    override def write(obj: E): JsValue = JsString(obj.map(_.entryName).mkString(","))

    override def read(json: JsValue): E = json match {
      case commaSeparatedStrings: JsString => {
        val invalidValues = commaSeparatedStrings.value.split(",").flatMap(s => values.find(v => v.entryName != s))
        val validValues = commaSeparatedStrings.value.split(",").flatMap(s => values.find(v => v.entryName == s))

        if (invalidValues.isEmpty && validValues.nonEmpty) {
          values
        } else {
          deserializationListError(JsString(invalidValues.mkString(",")))
        }
      }
      case x => deserializationListError(x)
    }

    private def deserializationListError(value: JsValue) = {
      val className = values.headOption.map(_.getClass.getEnclosingClass.getSimpleName).getOrElse("")
      throw DeserializationException(
        s"For '$className': Expected single or comma-separated values from enum $values but received invalid $value")
    }
  }

  implicit val additionalValidationFormat: EnumEntryJsonFormat[AdditionalValidation] =
    new EnumEntryJsonFormat[AdditionalValidation](Seq.empty)

  implicit val skipValidationFormat: ListEnumEntryJsonFormat[List[SkipValidation]] =
    new ListEnumEntryJsonFormat[List[SkipValidation]](SkipValidation.values.toList)

  implicit object TopicMetadataV2Format
      extends RootJsonFormat[TopicMetadataV2Request] {

    override def write(obj: TopicMetadataV2Request): JsValue =
      jsonFormat17(TopicMetadataV2Request.apply).write(obj)

    override def read(json: JsValue): TopicMetadataV2Request = json match {
      case j: JsObject =>
        val metadataValidationResult = MetadataOnlyRequestFormat.getValidationResult(json)
        val schemas = toResult(
          SchemasFormat.read(
            j.getFields("schemas")
              .headOption
              .getOrElse(
                throwDeserializationError(
                  "schemas",
                  "JsObject with key and value Avro Schemas"
                )
              )
          )
        )
        (schemas, metadataValidationResult) match {
          case (Valid(s), Valid(m)) => TopicMetadataV2Request.fromMetadataOnlyRequest(s, m)

          case (Invalid(es), Invalid(em)) =>
            throw DeserializationException(es.combine(em).map(_.errorMessage).mkString_(" "))

          case (Invalid(es), Valid(_)) =>
            throw DeserializationException(es.map(_.errorMessage).mkString_(" "))

          case (Valid(_), Invalid(em)) =>
            throw DeserializationException(em.map(_.errorMessage).mkString_(" "))
        }
      case j =>
        throw DeserializationException(invalidPayloadProvided(j))
    }
  }

  private class Converter(j: JsObject) {

    def toSubject(jsonField: String): Subject =
      SubjectFormat.read(j.getFields(jsonField).headOption.getOrElse(JsString.empty))

    def toStreamTypeV2(jsonField: String): StreamTypeV2 =
      StreamTypeV2Format.read(
        j.getFields(jsonField)
          .headOption
          .getOrElse(throwDeserializationError(jsonField, "String")))

    def toListOfContactMethods(jsonField: String): NonEmptyList[ContactMethod] =
      ContactFormat.read(
        j.getFields(jsonField)
          .headOption
          .getOrElse(throwDeserializationError(jsonField, "JsObject")))

    def toListOfStrings(jsonField: String): List[String] =
      j.fields.get(jsonField) match {
        case Some(t) => t.convertTo[Option[List[String]]].getOrElse(List.empty)
        case None => List.empty[String]
      }

    def toOptionalString(jsonField: String): Option[String] =
      j.fields.get(jsonField) match {
        case Some(teamName) => teamName.convertTo[Option[String]]
        case None => throwDeserializationError(jsonField, "String")
      }

    def toOptionalStringNoError(jsonField: String): Option[String] = j.getFields(jsonField).headOption.map(_.convertTo[String])

    def toOptionalNumPartitions(jsonField: String): Option[NumPartitions] =
      j.fields.get(jsonField).map { num =>
        TopicMetadataV2Request.NumPartitions.from(num.convertTo[Int]).toOption match {
          case Some(numP) => numP
          case None => throwDeserializationError(jsonField, "Int [10-50]")
        }
      }

    def toOptionalListOfStrings(jsonField: String): Option[List[String]] =
      j.fields.get(jsonField) match {
        case Some(t) => t.convertTo[Option[List[String]]]
        case None => None
      }
  }

  implicit object MetadataOnlyRequestFormat extends RootJsonFormat[MetadataOnlyRequest] {
    override def write(obj: MetadataOnlyRequest): JsValue = {
      JsString(obj.toString)
    }
    override def read(json: JsValue): MetadataOnlyRequest =  {
      getValidationResult(json) match {
          case Valid(metadataOnlyRequest) => metadataOnlyRequest
          case Invalid(e) =>
            throw DeserializationException(e.map(_.errorMessage).mkString_(" "))
        }
    }

    def getValidationResult(json: JsValue): MetadataValidationResult[MetadataOnlyRequest] = json match {
      case j: JsObject =>
        val c = new Converter(j)
        val subject = toResult(c.toSubject("subject"))
        val streamType = toResult(c.toStreamTypeV2("streamType"))
        val deprecatedFieldName = "deprecated"
        val deprecated = toResult(getBoolWithKey(j, deprecatedFieldName))
        val deprecatedDate = if ( deprecated.toOption.getOrElse(false) && !j.getFields("deprecatedDate").headOption.getOrElse(None).equals(None)) {
          toResult(Option(Instant.parse(j.getFields("deprecatedDate").headOption
            .getOrElse(throwDeserializationError("deprecatedDate","long"))
            .toString.replace("\"",""))))
        } else {
          toResult(None)
        }

        val payloadDataClassification = j.getFields("dataClassification").headOption
        val adaptedDataClassification = MetadataUtils.oldToNewDataClassification(payloadDataClassification)
        val dataClassification = toResult(
          new EnumEntryJsonFormat[DataClassification](DataClassification.values).read(
            adaptedDataClassification.getOrElse(throwDeserializationError("dataClassification", "String"))))

        val maybeSubDataClassification = pickSubDataClassificationValue(
          j.getFields("subDataClassification").headOption, payloadDataClassification)
        val subDataClassification = toResult(
          maybeSubDataClassification.map(new EnumEntryJsonFormat[SubDataClassification](SubDataClassification.values).read))

        val contact = toResult(c.toListOfContactMethods("contact"))
        val createdDate = toResult(Instant.now())
        val parentSubjects = toResult(c.toListOfStrings("parentSubjects"))
        val notes = toResult(c.toOptionalStringNoError("notes"))
        val teamName = toResult(c.toOptionalString("teamName"))
        val numPartitions = toResult(c.toOptionalNumPartitions("numPartitions"))
        val tags = toResult(c.toListOfStrings("tags"))
        val notificationUrl = toResult(c.toOptionalStringNoError("notificationUrl"))
        val replacementTopics = toResult(c.toOptionalListOfStrings("replacementTopics"))
        val previousTopics = toResult(c.toOptionalListOfStrings("previousTopics"))
        (
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
          numPartitions,
          tags,
          notificationUrl,
          toResult(None) // Never pick additionalValidations from the request.
          ).mapN(MetadataOnlyRequest.apply)
    }

    private def pickSubDataClassificationValue(payloadSubDataClassification: Option[JsValue],
                                                  payloadDataClassification: Option[JsValue]): Option[JsValue] =
      MetadataUtils.deriveSubDataClassification(payloadDataClassification).orElse(payloadSubDataClassification)
  }

  implicit object MaybeSchemasFormat extends RootJsonFormat[MaybeSchemas] {
    override def read(json: JsValue): MaybeSchemas = throw IntentionallyUnimplemented

    override def write(obj: MaybeSchemas): JsValue =  {
      val keyJson = ("key" -> obj.key.map(k => new SchemaFormat(isKey = true).write(k)).getOrElse(JsString("Unable to retrieve Key Schema")))
      val valueJson = ("value" -> obj.value.map(v => new SchemaFormat(isKey = false).write(v)).getOrElse(JsString("Unable to retrieve Value Schema")))

      JsObject(
        List(keyJson, valueJson).toMap
      )
    }
  }

  implicit object TopicMetadataResponseV2Format extends RootJsonFormat[TopicMetadataV2Response] {
    override def read(json: JsValue): TopicMetadataV2Response = throw IntentionallyUnimplemented

    override def write(obj: TopicMetadataV2Response): JsValue = jsonFormat16(TopicMetadataV2Response.apply).write(obj)
  }

  private def throwDeserializationError(key: String, `type`: String) =
    throw DeserializationException(MissingField(key, `type`).errorMessage)

  private def getBoolWithKey(json: JsObject, key: String): Boolean = {
    json
      .getFields(key)
      .headOption
      .exists(_.convertTo[Boolean])
  }
}

sealed trait TopicMetadataV2Validator {

  def toResult[A](a: => A): MetadataValidationResult[A] = {
    val v = Validated.catchNonFatal(a)
    v.toValidatedNec
      .leftMap[NonEmptyChain[ExceptionThrownOnParseWithException]] { es =>
        es.map(e => ExceptionThrownOnParseWithException(e.getMessage))
      }
  }

  type MetadataValidationResult[A] =
    ValidatedNec[TopicMetadataV2PayloadValidation, A]

}

sealed trait TopicMetadataV2PayloadValidation {
  def errorMessage: String
}

final case class ExceptionThrownOnParseWithException(message: String)
    extends TopicMetadataV2PayloadValidation {
  override def errorMessage: String = message
}

object Errors {

  def invalidPayloadProvided(actual: JsValue): String = {
    import spray.json._
    val expected =
      """
        |{
        |   "subject": "String a-zA-Z0-9.-\\",
        |   "schemas": {
        |     "key": {},
        |     "value": {}
        |   },
        |   "streamType": "oneOf History, Notification, Telemetry, CurrentState",
        |   "deprecated": false,
        |   "dataClassification": "Public",
        |   "contact": {
        |     "slackChannel" : "#channelName",
        |     "email" : "email@address.com"
        |   },
        |   "createdDate":"2020-01-20T12:34:56Z",
        |   "parentSubjects": ["subjectName"],
        |   "notes": "Optional - String Note"
        |}
        |""".stripMargin

    s"Expected payload like ${expected.parseJson.prettyPrint}, but received ${actual.prettyPrint}"
  }

  def invalidEmailProvided(value: JsValue) =
    s"Field `email` not recognized as a valid address, received ${value.compactPrint}."

  def invalidSlackChannelProvided(value: JsValue) =
    s"Field `slackChannel` must be all lowercase with no spaces and less than 80 characters, received ${value.compactPrint}."

  final case class InvalidSchema(value: JsValue, isKey: Boolean, error: Option[Throwable] = none) {

    def errorMessage: String = {
      s"${value.compactPrint} is not a properly formatted Avro Schema for field `${if (isKey) "key" else "value"}`." +
        s"${error.map(e => s"\nError: ${e.getMessage}\n").getOrElse("")}"
    }
  }

  final case class InvalidNamespace(reason: String) extends Throwable {
    override def getMessage: String = {
      s"One or more of the Namespaces provided are invalid due to: $reason"
    }
  }

  final case class InvalidSchemas(value: JsValue) {

    def errorMessage: String =
      s"Field Schemas must be an object containing a `key` avro schema and a `value` avro schema, received ${value.compactPrint}."
  }

  final case class IncompleteSchemas(combinedErrorMessages: String) {
    def errorMessage: String = combinedErrorMessages
  }

  case object ContactMissingContactOption {

    def errorMessage: String =
      """Field `contact` expects one or more of `email` or `slackChannel`."""
  }

  import scala.reflect.runtime.{universe => ru}

  final case class StreamTypeInvalid(
      value: JsValue,
      knownDirectSubclasses: Set[ru.Symbol]
  ) {

    def errorMessage: String =
      s"Field `streamType` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  import scala.reflect.runtime.{universe => ru}

  final case class DataClassificationInvalid(
      value: JsValue,
      knownDirectSubclasses: Set[ru.Symbol]
  ) {

    def errorMessage: String =
      s"Field `dataClassification` expected oneOf $knownDirectSubclasses, received ${value.compactPrint}"
  }

  final case class MissingField(field: String, fieldType: String) {
    def errorMessage: String = s"Field `$field` of type $fieldType"
  }

}
