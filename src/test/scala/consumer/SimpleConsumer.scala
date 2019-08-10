package zio.kafka.client

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.scalatest.{ Matchers, WordSpecLike }
import zio.{ Chunk, DefaultRuntime, RIO, UIO, ZIO }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

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
    r: Consumer[String, String] => RIO[Blocking with Clock, A]
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
        (consumer.subscribe(Subscription.Topics(Set(mytopic))) *> pollNtimes(10, consumer).map { records =>
          val kvs = (1 to 1).toArray.map(i => (s"msg$i"))
          val exp = Chunk.fromArray(kvs)
          val tmp = records.map(_.value)
          val act = Chunk.succeed(tmp)
          act.map(_ shouldEqual exp)
        })
      }
    }
  }
}
