package KafkaPkg

import zio.{ Chunk, DefaultRuntime, Task, TaskR, UIO }
import zio.kafka.client.{ Consumer, ConsumerSettings }
import zio.blocking.Blocking
import zio.clock.Clock

import org.apache.kafka.clients.consumer.{ ConsumerRecord }

final case class NetConfig(
  kafkaPort: Int,
  zooPort: Int
)

final case class SlaveConfig(
  server: String,
  client: String,
  group: String,
  topic: String
)

package object KafkaTypes {

  type BArr               = Array[Byte]
  type KafkaZIOData[K, V] = Chunk[ConsumerRecord[K, V]]
  type KafkaData          = KafkaZIOData[String, String]

  // Number of msgs to produce
  val msgCount = 2

  def genDummyListString: List[String] = {
    val res = (1 to msgCount).toList.map(i => s"msg$i")
    // res.foreach(println)
    res
  }

  def eqv(x: BArr, y: BArr): Boolean = java.util.Arrays.equals(x, y)

}

sealed abstract class KafkaBase extends DefaultRuntime

abstract class KafkaConsumer extends KafkaBase {
  // import KafkaTypes._

  type WorkerType[A] = Consumer[String, String] => TaskR[Blocking with Clock, A]

  def settings(cfg: SlaveConfig): ConsumerSettings
  def run[A](cfg: SlaveConfig)(r: WorkerType[A]): A
  def subscribe(cfg: SlaveConfig): Task[Unit]
  def peekBatch(cfg: SlaveConfig): Chunk[String]
  def readBatch(cfg: SlaveConfig): Chunk[String]
  def produce(cfg: SlaveConfig): UIO[Unit]
  // def produceAndConsume(cfg: SlaveConfig): KafkaData

}

abstract class KafkaProducer extends KafkaBase
