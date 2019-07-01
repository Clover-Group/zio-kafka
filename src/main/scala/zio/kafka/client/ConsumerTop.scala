package kafkaconsumer

import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.clients.consumer.ConsumerConfig

import zio.{ Chunk, DefaultRuntime, TaskR }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import zio.kafka.client.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.client.KafkaTestUtils.{ pollNtimes }

final case class ConnectionSetup(
  server: String,
  client: String,
  group: String,
  topic: String
)

sealed abstract class KafkaConsumer extends DefaultRuntime {

  type WorkerType[A] = Consumer[String, String] => TaskR[Blocking with Clock, A]

  def settings(server: String, groupId: String, clientId: String): ConsumerSettings
  def run[A](server: String, groupId: String, clientId: String)(r: WorkerType[A]): A
  // def subscribe(server: String, groupId: String, clientId: String, topic: String): Task[Unit]
  // def unsubscribe(server: String, groupId: String, clientId: String): Task[Unit]
  // def poll(server: String, groupId: String, clientId: String, topic: String): Chunk[String]

  def readBatch(cfg: ConnectionSetup): Chunk[String]

}

object KafkaConsumer extends KafkaConsumer {

  implicit val stringSerde: Serde[String] = Serdes.String()

  def settings(server: String, groupId: String, clientId: String): ConsumerSettings =
    ConsumerSettings(
      List(server),
      groupId,
      clientId,
      3.seconds,
      Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    )

  def run[A](server: String, groupId: String, clientId: String)(r: WorkerType[A]): A =
    unsafeRun(
      Consumer.make[String, String](settings(server, groupId, clientId)).use(r)
    )

  /* def subscribe(server: String, groupId: String, clientId: String, topic: String): Task[Unit] =
    run(server, groupId, clientId) { consumer =>
      for {
        outcome <- consumer.subscribe(Subscription.Topics(Set(topic)))
      } yield ZIO.effect(outcome)

    }

  def unsubscribe(server: String, groupId: String, clientId: String): Task[Unit] =
    run(server, groupId, clientId) { consumer =>
      for {
        outcome <- consumer.unsubscribe
      } yield ZIO.effect(outcome)
    }

  def poll(server: String, groupId: String, clientId: String, topic: String): Chunk[String] =
    run(server, groupId, clientId) { consumer =>
      for {
        recs <- pollNtimes(10, consumer)
        tmp  = recs.map(_.value)
      } yield tmp
    } */

  def readBatch(cfg: ConnectionSetup): Chunk[String] =
    run(cfg.server, cfg.group, cfg.client) { consumer =>
      (consumer.subscribe(Subscription.Topics(Set(cfg.topic))) *> pollNtimes(10, consumer)).map { recs =>
        for {
          tmp <- recs.map(_.value)
        } yield tmp
      }

    }

}
