package zio.kafka.client

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import zio.{ Chunk, UIO, ZIO }
import zio.blocking.Blocking
import zio.duration._

object KafkaTestUtils {

  def produceOne[A](t: String, m: A): UIO[Unit] = ZIO.effectTotal {
    import net.manub.embeddedkafka.Codecs._
    EmbeddedKafka.publishToKafka[A](t, m)

  }

  /* def produceMany[F[_], A](t: String, m: F[A]): UIO[Unit] = //UIO.unit
    UIO.foreach(kvs)(i => produceOne[A](t, m)).unit */

  def produceMany[A](t: String, m: List[A]): UIO[Unit] = //UIO.unit
    UIO.foreach(m)(i => produceOne[A](t, i)).unit

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
