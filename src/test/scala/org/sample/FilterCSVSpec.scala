package org.sample

import zio.Chunk
import zio.stream.{Sink, ZStream}
import zio.test._

import java.time.LocalDateTime
import scala.concurrent.duration.DurationInt

object FilterCSVSpec extends DefaultRunnableSpec {
  def stream = ZStream.fromIterable(Seq(
    "test0,1.1.1.1,2015-12-01 06:34:59",
    "test1,1.1.1.1,2015-12-01 06:35:00",
    "test2,1.1.1.1,2015-12-01 06:37:00",
    "test2,1.1.1.1,2015-12-01 06:39:00",
    "test3,1.1.1.1,2015-12-01 06:39:59",
    "test3,1.1.1.2,2015-12-01 06:42:00",
  ))

  override def spec = suite("FilterCSV")(
    testM("process with 5 minutes window correctly") {
      val sink = Sink.collectAll[IpAddressDetails]
      for {
        runner <- FilterCSV.process(stream, 5.minutes).run(sink)
      } yield {
        assertTrue(runner == Chunk(
          IpAddressDetails(
            "1.1.1.1",
            LocalDateTime.parse("2015-12-01 06:35:00", FilterCSV.dateTimeFormatter),
            LocalDateTime.parse("2015-12-01 06:39:59", FilterCSV.dateTimeFormatter),
            List(
              buildLoginStat("test1", "2015-12-01 06:35:00"),
              buildLoginStat("test2", "2015-12-01 06:37:00"),
              buildLoginStat("test2", "2015-12-01 06:39:00"),
              buildLoginStat("test3", "2015-12-01 06:39:59")
            )
          )
        ))
      }
    },
    testM("process with 10 minutes window correctly") {
      val sink = Sink.collectAll[IpAddressDetails]
      for {
        runner <- FilterCSV.process(stream, 10.minutes).run(sink)
      } yield {
        assertTrue(runner == Chunk(
          IpAddressDetails(
            "1.1.1.1",
            LocalDateTime.parse("2015-12-01 06:34:59", FilterCSV.dateTimeFormatter),
            LocalDateTime.parse("2015-12-01 06:39:59", FilterCSV.dateTimeFormatter),
            List(
              buildLoginStat("test0", "2015-12-01 06:34:59"),
              buildLoginStat("test1", "2015-12-01 06:35:00"),
              buildLoginStat("test2", "2015-12-01 06:37:00"),
              buildLoginStat("test2", "2015-12-01 06:39:00"),
              buildLoginStat("test3", "2015-12-01 06:39:59")
            )
          )
        ))
      }
    }
  )

  private def buildLoginStat(login: String, dateTime: String) = s"$login:${LocalDateTime.parse(dateTime, FilterCSV.dateTimeFormatter)}"
}
