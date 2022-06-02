package org.sample

import zio.blocking.Blocking
import zio.console._
import zio.stream.{ZSink, ZStream}
import zio.{ExitCode, Task, URIO, ZManaged}

import java.nio.file.{Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.util.matching.Regex

object FilterCSV extends zio.App {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val userLoginRegexp: Regex = raw""""?(.+[^"])"?,"?([\d\\.]+)"?,"?([\d\-:\s]+)"?""".r

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    (for {
      _ <- putStrLn("Enter duration window (ex: \"1 hour\"):")
      timePeriod <- getStrLn.map(_.split(" ")).map { case Array(t, a) => FiniteDuration(t.toInt, a) }
      stream = managedStream("logins0.csv")
      _ <- putStrLn("Filtering...")
      result = process(stream, timePeriod)
      _ <- result.map(toRaw).intersperse("\n").run(fileSink(Paths.get("test.csv")))
      _ <- putStrLn("Finished.")
    } yield ()).exitCode

  def process(inputStream: ZStream[Blocking, Throwable, String], timeRange: FiniteDuration): ZStream[Blocking, Throwable, IpAddressDetails] =
    inputStream
      .map(fromRaw)
      .collect { case Some(e) => e } //todo: add processing errors
      .map(userInfo => userInfo.dateTime.toEpochSecond(ZoneOffset.UTC) / timeRange.toSeconds -> userInfo)
      .mapAccum(Option.empty[(Long, List[UserInfo])]) { case (acc, (timeBucket, userInfo)) =>
        acc match {
          case None =>
            (Some(timeBucket, List(userInfo)), None)
          case Some((groupedTimeBucket, elements)) if groupedTimeBucket == timeBucket =>
            (Some(timeBucket, userInfo :: elements), None)
          case Some((_, elements)) => (Some(timeBucket, List(userInfo)), Some(elements))
        }
      }
      .collect {
        case Some(userInfos) =>
          userInfos.groupBy(_.ip).filter(_._2.size > 1).map { case (ip, userInfos) =>
            IpAddressDetails(
              ip,
              userInfos.last.dateTime,
              userInfos.head.dateTime,
              userInfos.foldLeft(List.empty[String])((a, e) => s"${e.login}:${e.dateTime}" :: a))
          }
      }.mapConcat(identity)


  def fileSink(path: Path): ZSink[Blocking, Throwable, String, Byte, Long] =
    ZSink.fromFile(path).contramapChunks[String](_.flatMap(_.getBytes))

  private def managedStream(path: String): ZStream[Blocking, Throwable, String] =
    ZStream.fromIteratorManaged(ZManaged.fromAutoCloseable(Task(Source.fromResource(path))).map(_.getLines()))

  private def fromRaw(r: String): Option[UserInfo] = {
    r match {
      case userLoginRegexp(login, ip, dateTime) => Some(UserInfo(login, ip, LocalDateTime.parse(dateTime, dateTimeFormatter)))
      case _ => None
    }
  }

  private def toRaw(details: IpAddressDetails) = {
    s""""${
      details.ip
    }","${
      details.start
    }","${
      details.end
    }","${
      details.users.mkString(",")
    }""""
  }
}

case class DateTimeRange(start: LocalDateTime, end: LocalDateTime)

case class UserInfo(login: String, ip: String, dateTime: LocalDateTime)

case class IpAddressDetails(ip: String, start: LocalDateTime, end: LocalDateTime, users: List[String])

