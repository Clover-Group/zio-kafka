package kafkaconsumer

import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.clients.consumer.ConsumerConfig

import zio.{ Chunk, DefaultRuntime, Task, TaskR, ZIO }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import zio.kafka.client.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.client.KafkaTestUtils.{ pollNtimes }

final case class ConnectionConfig(
  server: String,
  client: String,
  group: String,
  topic: String
)

sealed abstract class KafkaConsumer extends DefaultRuntime {

  type WorkerType[A] = Consumer[String, String] => TaskR[Blocking with Clock, A]

  def settings(cfg: ConnectionConfig): ConsumerSettings
  def run[A](cfg: ConnectionConfig)(r: WorkerType[A]): A
  def subscribe(cfg: ConnectionConfig): Task[Unit]
  // def unsubscribe(cfg: ConnectionConfig): Task[Unit]
  // def readBatch(cfg: ConnectionConfig): Chunk[String]
  def peekBatch(cfg: ConnectionConfig): Chunk[String]
  def readBatch(cfg: ConnectionConfig): Chunk[String]

}

object KafkaConsumer extends KafkaConsumer {

  implicit val stringSerde: Serde[String] = Serdes.String()

  def settings(cfg: ConnectionConfig): ConsumerSettings =
    ConsumerSettings(
      List(cfg.server),
      cfg.group,
      cfg.client,
      3.seconds,
      Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    )

  def run[A](cfg: ConnectionConfig)(r: WorkerType[A]): A =
    unsafeRun(
      Consumer.make[String, String](settings(cfg)).use(r)
    )

  def subscribe(cfg: ConnectionConfig): Task[Unit] =
    run(cfg) { consumer =>
      for {
        outcome <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
      } yield ZIO.effect(outcome)

    }

  /* def unsubscribe(cfg: ConnectionConfig): Task[Unit] =
    run(cfg) { consumer =>
      for {
        outcome <- consumer.unsubscribe
      } yield ZIO.effect(outcome)
    }

  def readBatch(cfg: ConnectionConfig): Chunk[String] =
    run(cfg) { consumer =>
      (consumer.subscribe(Subscription.Topics(Set(cfg.topic))) *> pollNtimes(5, consumer)).map { recs =>
        for {
          tmp <- recs.map(_.value)
        } yield tmp
      }

    } */

  def peekBatch(cfg: ConnectionConfig): Chunk[String] =
    run(cfg) { consumer =>
      for {
        _     <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
        _     = println("subscribe done")
        batch <- pollNtimes(5, consumer)
        _     = println("poll done")
        data  = batch.map(_.value)
        size  = data.toArray.size
        _     = println(s"data size = $size")
        //_     <- consumer.unsubscribe
        //_ = println ("unsubscribe done")
      } yield data

    }

  def readBatch(cfg: ConnectionConfig): Chunk[String] =
    run(cfg) { consumer =>
      for {
        _     <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
        batch <- pollNtimes(5, consumer)
        data  = batch.map(_.value)
        size  = data.toArray.size
        _     = println(s"data size = $size")
        //_     <- consumer.unsubscribe

      } yield data

    }

}
