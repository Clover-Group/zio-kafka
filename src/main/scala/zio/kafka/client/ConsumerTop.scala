package kafkaconsumer

import org.apache.kafka.common.serialization.{ Serde, Serdes }
import org.apache.kafka.clients.consumer.ConsumerConfig

import zio.{ DefaultRuntime, TaskR }
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import zio.kafka.client.{ Consumer, ConsumerSettings }

sealed abstract class KafkaConsumer extends DefaultRuntime {

  type WorkerType[A] = Consumer[String, String] => TaskR[Blocking with Clock, A]

  def settings(server: String, groupId: String, clientId: String): ConsumerSettings
  def run[A](server: String, groupId: String, clientId: String)(r: WorkerType[A]): A

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

  val server = s"localhost:9092"

  def run[A](server: String, groupId: String, clientId: String)(r: WorkerType[A]): A =
    unsafeRun(
      Consumer.make[String, String](settings(server, groupId, clientId)).use(r)
    )

}
