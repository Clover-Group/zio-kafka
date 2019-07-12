package zio.kafka.client

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import zio.{ Chunk, Task, UIO, ZIO }
import zio.blocking.Blocking
import zio.duration._

import KafkaPkg.{ NetConfig }

object KafkaTestUtils {

  def produceOne(cfg: NetConfig, t: String, k: String, m: String): UIO[Unit] = ZIO.effectTotal {
    import net.manub.embeddedkafka.Codecs.{ stringSerializer }
    val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)
    EmbeddedKafka.publishToKafka(t, k, m)(lcfg, stringSerializer, stringSerializer)
  }

  def produceMany(cfg: NetConfig, t: String, kvs: List[(String, String)]): UIO[Unit] =
    UIO.foreach(kvs)(i => produceOne(cfg, t, i._1, i._2)).unit

  // def produceMany[F[_], A](t: String, m: F[A]): UIO[Unit] = //UIO.unit
  // UIO.foreach(kvs)(i => produceOne[A](t, m)).unit

  // def produceMany[F[_] <: List, A](t: String, m: F[A]): UIO[Unit] = //UIO.unit
  // UIO.foreach(m)(i => produceOne(t, i._1, i._2)).unit

  def produceChunk(cfg: NetConfig, t: String, data: Array[Byte]): Task[Unit] = ZIO.effect {
    import net.manub.embeddedkafka.Codecs.{ nullSerializer }
    // implicit val cfg = EmbeddedKafkaConfig(kafkaPort = port)
    val lcfg = EmbeddedKafkaConfig(kafkaPort = cfg.kafkaPort, zooKeeperPort = cfg.zooPort)
    EmbeddedKafka.publishToKafka[Array[Byte]](t, data)(lcfg, nullSerializer)
  }

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
