package zio.kafka

import java.util.{ Map => JMap }

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import zio.ZIO
import zio.blocking.Blocking

package object client {
  type BlockingTask[A] = ZIO[Blocking, Throwable, A]

  type OffsetMap = Map[TopicPartition, OffsetAndMetadata]

  type JOffsetMap = JMap[TopicPartition, OffsetAndMetadata]
}
