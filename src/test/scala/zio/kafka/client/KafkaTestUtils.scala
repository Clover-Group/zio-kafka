package zio.kafka.client

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import zio.{ Chunk, Task, UIO, ZIO }
import zio.blocking.Blocking
import zio.duration._

import KafkaPkg.{ NetConfig }
// import cats.Functor

object KafkaTestUtils {

  type BArr = Array[Byte]

  trait HowToProduce[A] {
    def apply(cfg: NetConfig, t: String, data: Chunk[A]): UIO[Unit]
  }

  /* object HowToProduce {

    def apply[A](cfg: NetConfig, t: String, data: Chunk[A]): UIO[Unit] = ZIO.effectTotal {
      import net.manub.embeddedkafka.Codecs.{ nullSerializer, stringSerializer }
      val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)

      println("produceOne called")
      data match {
        case din: Chunk[String] => din.map(d => EmbeddedKafka.publishToKafka[String](t, d)(lcfg, stringSerializer))
        case din: Chunk[BArr]   => din.map(d => EmbeddedKafka.publishToKafka[BArr](t, d)(lcfg, nullSerializer))
        case _                  => ZIO.fail("Unknown input type")

      }
      println("produceOne returned")

    }

  } */
  private def produceOne[A](cfg: NetConfig, t: String, data: Chunk[A]): UIO[Unit] = ZIO.effectTotal {

    import net.manub.embeddedkafka.Codecs.{ nullSerializer, stringSerializer }
    val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)

    println("produceOne called")
    data match {
      // case din: Chunk[String] => din.map(d => EmbeddedKafka.publishToKafka[String](t, d)(lcfg, stringSerializer))
      case din: Chunk[BArr] => din.map(d => EmbeddedKafka.publishToKafka[BArr](t, d)(lcfg, nullSerializer))
      case _                => ZIO.fail("Unknown input type")

    }
    println("produceOne returned")
  }
  /* private def produceOne[A](cfg: NetConfig, t: String, data: Chunk[A])(htp: HowToProduce[A]): UIO[Unit] =
    htp(cfg, t, data) */

  def produce[A](cfg: NetConfig, t: String, data: Chunk[A]): UIO[Unit] =
    // data.mapM_(_ => produceOne[A](cfg, t, data)(HowToProduce[A]))
    data.mapM_(_ => produceOne[A](cfg, t, data))

  def recordsFromAllTopics[K, V](
    pollResult: Map[TopicPartition, Chunk[ConsumerRecord[K, V]]]
  ): Chunk[ConsumerRecord[K, V]] =
    Chunk.fromIterable(pollResult.values).flatMap(identity)

  def getAllRecordsFromMultiplePolls[K, V](
    res: List[Map[TopicPartition, Chunk[ConsumerRecord[K, V]]]]
  ): Chunk[ConsumerRecord[K, V]] =
    res.foldLeft[Chunk[ConsumerRecord[K, V]]](Chunk.empty)(
      (acc, pollResult) => acc ++ recordsFromAllTopics[K, V](pollResult)
    )

  def pollNtimes[K, V](n: Int, consumer: Consumer[K, V]): ZIO[Blocking, Throwable, Chunk[ConsumerRecord[K, V]]] =
    ZIO.foreach(List.fill(n)(()))(_ => consumer.poll(1.second)).map(getAllRecordsFromMultiplePolls)

  def tp(topic: String, partition: Int): TopicPartition = new TopicPartition(topic, partition)
}
