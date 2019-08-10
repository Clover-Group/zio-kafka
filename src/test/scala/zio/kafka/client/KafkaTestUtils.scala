package zio.kafka.client

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import zio.{ Chunk, Task, ZIO }
// import zio.console.{ putStrLn }
import zio.blocking.Blocking
import zio.duration._

import KafkaPkg.{ NetConfig }

object KafkaTestUtils {

  type BArr = Array[Byte]

  sealed abstract class BackProducer[A] {
    def apply(cfg: NetConfig, t: String, data: A): Task[Unit]
  }

  object BackProducer {

    implicit val stringProducer = new BackProducer[String] {

      def apply(cfg: NetConfig, t: String, data: String): Task[Unit] = ZIO.effect {

        import net.manub.embeddedkafka.Codecs.{ stringSerializer }
        val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)

        EmbeddedKafka.publishToKafka[String](t, data)(lcfg, stringSerializer)
      }
    }

    implicit val BarrProducer = new BackProducer[BArr] {

      def apply(cfg: NetConfig, t: String, data: BArr): Task[Unit] = ZIO.effect {

        import net.manub.embeddedkafka.Codecs.{ nullSerializer }
        val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)

        EmbeddedKafka.publishToKafka[BArr](t, data)(lcfg, nullSerializer)
      }
    }
  }

  def produce[A](cfg: NetConfig, t: String, data: Chunk[A])(implicit prod: BackProducer[A]): Task[Unit] =
    data.mapM_(r => prod(cfg, t, r))

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
