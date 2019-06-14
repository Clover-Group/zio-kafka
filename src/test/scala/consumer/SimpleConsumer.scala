package zio.kafka.client

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.scalatest.{ Matchers, WordSpecLike }
import scalaz.zio.{ Chunk, DefaultRuntime, TaskR, UIO, ZIO }
import scalaz.zio.blocking.Blocking
import scalaz.zio.clock.Clock
import scalaz.zio.duration._

class SimpleConsumer extends WordSpecLike with Matchers with LazyLogging with DefaultRuntime {
  import KafkaTestUtils._

  def pause(): ZIO[Clock, Nothing, Unit] = UIO(()).delay(2.seconds).forever

  def log(s: String): UIO[Unit] = ZIO.effectTotal(logger.info(s))

  val bootstrapServer                     = s"localhost:9092"
  implicit val stringSerde: Serde[String] = Serdes.String()

  def settings(groupId: String, clientId: String) =
    ConsumerSettings(
      List(bootstrapServer),
      groupId,
      clientId,
      3.seconds,
      Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    )

  def runWithConsumer[A](groupId: String, clientId: String)(
    r: Consumer[String, String] => TaskR[Blocking with Clock, A]
  ): A =
    unsafeRun(
      Consumer.make[String, String](settings(groupId, clientId)).use(r)
    )

  val mytopic = "testTopic"

  val groupID  = "10"
  val clientID = "client0"

  "A string consumer" can {
    "subscribe" should {
      "to a single topic with non empty name" in runWithConsumer(groupID, clientID) { consumer =>
        for {
          outcome <- consumer.subscribe(Subscription.Topics(Set(mytopic))).either
          _       <- ZIO.effect(outcome.isRight shouldBe true)
        } yield ()
      }
    }

    "poll" should {
      "receive messages produced on the topic" in runWithConsumer(groupID, clientID) { consumer =>
        for {
          _   <- consumer.subscribe(Subscription.Topics(Set(mytopic)))
          kvs = (1 to 1).toArray.map(i => (s"msg$i"))
          exp = Chunk.fromArray(kvs)

          records <- pollNtimes(10, consumer)
          tmp = records.map { r =>
            r.value
          }
          act = Chunk.succeed(tmp)
        } yield (act.map(a => a shouldEqual exp))
      }
    }
  }
}
