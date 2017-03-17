package hydra.ingest.services

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActor, TestActorRef, TestKit, TestProbe}
import hydra.core.ingest.{HydraRequest, IngestionParams}
import hydra.core.protocol.{Join, Publish, ValidRequest, Validate}
import hydra.ingest.services.IngestionSupervisor.InitiateIngestion
import hydra.ingest.services.IngestorRegistry.{IngestorLookupResult, Lookup}
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/9/17.
  */
class IngestionSupervisorSpec extends TestKit(ActorSystem("hydra")) with Matchers with FunSpecLike with ImplicitSender {

  val ingestor = TestProbe()
  ingestor.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
      msg match {
        case Publish(_) => sender ! Join; TestActor.KeepRunning
        case Validate(request) => sender ! ValidRequest; TestActor.KeepRunning
      }
  })

  val registry = TestProbe()
  registry.setAutoPilot(new TestActor.AutoPilot {
    def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
      msg match {
        case p@Publish(_) => ingestor.ref.tell(p, sender); TestActor.KeepRunning
        case Lookup(_) => sender ! ingestor; TestActor.KeepRunning
      }
  })

  val request = HydraRequest(Some("test"), "test payload")

  val ingestorRequest = request.withMetadata(IngestionParams.HYDRA_INGESTOR_TARGET_PARAM -> "test_ingestor")

  describe("When supervising an ingestion") {
    it("publishes a request") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(registry.ref, request, 1.second))
      ingestionSupervisor ! InitiateIngestion
      registry.expectMsgType[Publish]
    }

    it("looks up a target ingestor instead of publishing") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(registry.ref, ingestorRequest, 1.second))
      ingestionSupervisor ! InitiateIngestion
      registry.expectMsgType[Lookup]
      expectMsgType[IngestorLookupResult]
    }

    it("sends a validate message to the ingestor") {
      val ingestionSupervisor = TestActorRef(IngestionSupervisor.props(registry.ref, request, 1.second))
      ingestionSupervisor ! InitiateIngestion
      ingestor.expectMsgType[Publish]
      ingestor.expectMsgType[Validate]
    }

  }

}