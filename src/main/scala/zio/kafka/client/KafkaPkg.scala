package KafkaPkg

import zio.{ Chunk, DefaultRuntime, Task, TaskR, UIO }
import zio.kafka.client.{ Consumer, ConsumerSettings }
import zio.blocking.Blocking
import zio.clock.Clock

import org.apache.kafka.clients.consumer.{ ConsumerRecord }

final case class ConnectionConfig(
  server: String,
  client: String,
  group: String,
  topic: String
)

object KafkaTypes {

  type KafkaZIOData[K, V] = Chunk[ConsumerRecord[K, V]]
  type KafkaData          = KafkaZIOData[String, String]

}

sealed abstract class KafkaBase extends DefaultRuntime

abstract class KafkaConsumer extends KafkaBase {
  import KafkaTypes._

  type WorkerType[A] = Consumer[String, String] => TaskR[Blocking with Clock, A]

  def settings(cfg: ConnectionConfig): ConsumerSettings
  def run[A](cfg: ConnectionConfig)(r: WorkerType[A]): A
  def subscribe(cfg: ConnectionConfig): Task[Unit]
  def peekBatch(cfg: ConnectionConfig): Chunk[String]
  def readBatch(cfg: ConnectionConfig): Chunk[String]
  def produce(cfg: ConnectionConfig): UIO[Unit]
  def produceAndConsume(cfg: ConnectionConfig): KafkaData

}

abstract class KafkaProducer extends KafkaBase
