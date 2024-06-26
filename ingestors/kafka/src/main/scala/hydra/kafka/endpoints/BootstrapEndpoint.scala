/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.kafka.endpoints

import java.time.Instant
import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, extractExecutionContext}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import hydra.common.config.KafkaConfigUtils.{KafkaClientSecurityConfig, SchemaRegistrySecurityConfig}
import hydra.common.logging.LoggingAdapter
import hydra.common.util.Futurable
import hydra.core.http.security.{AccessControlService, AwsSecurityService}
import hydra.core.http.security.AwsIamPolicyAction.KafkaAction
import hydra.core.http.{CorsSupport, DefaultCorsSupport, HydraDirectives, RouteSupport}
import hydra.core.marshallers.TopicMetadataRequest
import hydra.core.monitor.HydraMetrics.addHttpMetric
import hydra.kafka.model.TopicMetadataAdapter
import hydra.kafka.services.TopicBootstrapActor._
import hydra.kafka.util.MetadataUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class BootstrapEndpoint[F[_]: Futurable](override val system:ActorSystem,
                        override val streamsManagerActor: ActorRef,
                        override val kafkaClientSecurityConfig: KafkaClientSecurityConfig,
                        override val schemaRegistrySecurityConfig: SchemaRegistrySecurityConfig,
                        override val auth: AccessControlService[F],
                        override val awsSecurityService: AwsSecurityService[F])(implicit val corsSupport: CorsSupport) extends RouteSupport
  with LoggingAdapter
  with TopicMetadataAdapter
  with HydraDirectives
  with DefaultCorsSupport
  with BootstrapEndpointActors[F] {

  private implicit val timeout = Timeout(10.seconds)

  override val route: Route = cors(corsSupport.settings) {
    extractMethod { method =>
      handleExceptions(exceptionHandler("Bootstrap", Instant.now, method.value)) {
        extractExecutionContext { implicit ec =>
          pathPrefix("streams") {
            val startTime = Instant.now
            pathEndOrSingleSlash {
              post {
              auth.mskAuth(KafkaAction.CreateTopic) { roleName =>
                requestEntityPresent {
                  entity(as[TopicMetadataRequest]) { topicMetadataRequest =>
                    val dc = topicMetadataRequest.dataClassification
                    val transformedTopicMetadataRequest = topicMetadataRequest
                      .updateDataClassification(MetadataUtils.oldToNewDataClassification(dc))
                      .updateSubDataClassification(MetadataUtils.deriveSubDataClassification(dc))
                    val topic = transformedTopicMetadataRequest.schema.getFields("namespace", "name").mkString(".").replaceAll("\"", "")
                    onComplete(
                      bootstrapActor ? InitiateTopicBootstrap(transformedTopicMetadataRequest)
                    ) {

                      case Success(message) =>
                        message match {

                          case BootstrapSuccess(metadata) =>
                            addHttpMetric(topic, StatusCodes.OK, "Bootstrap", startTime, "POST")
                            if (roleName.isDefined) Futurable[F].unsafeToFuture(awsSecurityService.addAllTopicPermissionsPolicy(metadata.subject, roleName.get))
                            complete(StatusCodes.OK, toResource(metadata))

                          case BootstrapFailure(reasons) =>
                            addHttpMetric(topic, StatusCodes.BadRequest, "Bootstrap", startTime, "POST", error = Some(reasons.toString))
                            complete(StatusCodes.BadRequest, reasons)

                          case e: Exception =>
                            log.error("Unexpected error in TopicBootstrapActor", e)
                            addHttpMetric(topic, StatusCodes.InternalServerError, "Bootstrap", startTime, "POST", error = Some(e.getMessage))
                            complete(StatusCodes.InternalServerError, e.getMessage)
                        }

                      case Failure(ex) =>
                        log.error("Unexpected error in BootstrapEndpoint", ex)
                        addHttpMetric(topic, StatusCodes.InternalServerError, "Bootstrap", startTime, "POST", error = Some(ex.getMessage))
                        complete(StatusCodes.InternalServerError, ex.getMessage)
                    }
                  }
                }
              }
              }
            } ~ get {
              pathEndOrSingleSlash(getAllStreams(None, startTime)) ~
                path(Segment)(subject => getAllStreams(Some(subject), startTime))
            }
          }
        }
      }
    }
  }

  private def getAllStreams(subject: Option[String], startTime: Instant): Route = {
    extractExecutionContext { implicit ec =>
      onSuccess(bootstrapActor ? GetStreams(subject)) {
        case GetStreamsResponse(metadata) =>
          addHttpMetric("", StatusCodes.OK, "getAllStreams", startTime, "GET")
          complete(StatusCodes.OK, metadata.map(toResource))
        case Failure(ex) =>
          addHttpMetric("", StatusCodes.OK, "getAllStreams", startTime, "GET", error = Some(ex.getMessage))
          throw ex
        case x =>
          log.error("Unexpected error in BootstrapEndpoint", x)
          addHttpMetric("", StatusCodes.InternalServerError, "getAllStreams", startTime, "GET", error = Some(x.toString))
          complete(StatusCodes.InternalServerError, "Unknown error")
      }
    }
  }

  private def exceptionHandler(topic: String, startTime: Instant, method: String) = ExceptionHandler {
    case e =>
      extractExecutionContext { implicit ec =>
        addHttpMetric(topic, StatusCodes.InternalServerError,"Bootstrap", startTime, method, error = Some(e.getMessage))
        complete(500, e.getMessage)
      }
  }
}