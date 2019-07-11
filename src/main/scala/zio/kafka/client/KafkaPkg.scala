package KafkaPkg

import zio.{ Chunk, DefaultRuntime, Task, TaskR, UIO }
import zio.kafka.client.{ Consumer, ConsumerSettings }
import zio.blocking.Blocking
import zio.clock.Clock

import org.apache.kafka.clients.consumer.{ ConsumerRecord }

/* final case class ConnectionConfig[F[_], A](
  server: F[A],
  client: A,
  group : A,
  topic : A
) */
final case class ConnectionConfig(
  server: String,
  client: String,
  group: String,
  topic: String
)

package object KafkaTypes {

  type BArr               = Array[Byte]
  type KafkaZIOData[K, V] = Chunk[ConsumerRecord[K, V]]
  type KafkaData          = KafkaZIOData[String, String]
  type DummyData          = List[(String, String)]

  // Number of msgs to produce
  val msgCount = 2

  def genDummyData: DummyData =
    (1 to msgCount).toList.map(i => (s"key$i", s"msg$i"))

  object BArrEq extends cats.kernel.Eq[BArr] {
    def eqv(x: Array[Byte], y: Array[Byte]): Boolean = java.util.Arrays.equals(x, y)
  }

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
