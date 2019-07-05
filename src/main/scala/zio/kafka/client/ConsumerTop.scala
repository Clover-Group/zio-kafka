package kafkaconsumer

import KafkaPkg._
import KafkaTypes._

import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.clients.consumer.ConsumerConfig

import zio.{ Chunk, Task, UIO, ZIO }
import zio.duration._

import zio.kafka.client.{ Consumer, ConsumerSettings, Subscription }
import zio.kafka.client.KafkaTestUtils.{ pollNtimes, produceMany }

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

  def peekBatch(cfg: ConnectionConfig): Chunk[String] =
    run(cfg) { consumer =>
      for {
        _     <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
        _     = println(s"subscribe done at host: $cfg.server, port: $cfg.port, topic: $cfg.topic")
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

  def produce(cfg: ConnectionConfig): UIO[Unit] =
    run(cfg) { consumer =>
      for {
        _   <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
        kvs <- ZIO(genDummyData)
        _   <- produceMany(cfg.topic, kvs)
      } yield ZIO.unit
    }

  def produceAndConsume(cfg: ConnectionConfig): KafkaData =
    run(cfg) { consumer =>
      for {
        _       <- consumer.subscribe(Subscription.Topics(Set(cfg.topic)))
        kvs     <- ZIO(genDummyData)
        _       <- produceMany(cfg.topic, kvs)
        records <- pollNtimes(10, consumer)
      } yield records
    }

}
