package com.day28.booking

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Day 28 - Real-Time Booking Project (Scala + Spark Streaming / DStreams)
 *
 * Pipeline:
 *   lines -> parse + validate (broadcast reference)
 *         -> Pair RDD (booking_id, event) -> updateStateByKey      = booking state
 *         -> Pair RDD ((category, resource), units) -> reduceByKey = occupancy / availability
 *         -> Pair RDD ((category, resource), WindowStats) -> reduceByKeyAndWindow (with inverse)
 *         -> Spark SQL temp views -> reports
 */
object BookingStreamingApp {

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.parse(args)

    val conf = new SparkConf()
      .setAppName("Day28-Real-Time-Booking-Project")
      .set("spark.sql.shuffle.partitions", cfg.shufflePartitions.toString)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    if (!conf.contains("spark.master")) conf.setMaster(cfg.master)

    val ssc = new StreamingContext(conf, Seconds(cfg.batchSeconds))
    val sc  = ssc.sparkContext
    sc.setLogLevel("WARN")
    ssc.checkpoint(cfg.checkpoint) // mandatory for updateStateByKey and inverse-reduce windows

    // ---- 1. Broadcast reference data (capacity, price, name, city) ----------------------
    val refB = sc.broadcast(ReferenceData.load(sc, cfg.reference))
    println(s"Broadcast reference data: ${refB.value.size} resources")

    // ---- 2. Source -----------------------------------------------------------------------
    val (rawLines, replayBatches) = cfg.source.toLowerCase match {
      case "socket" => (ssc.socketTextStream(cfg.host, cfg.port), -1)
      case _        => replayStream(ssc, cfg)
    }
    val lines = rawLines.filter(l => l.trim.nonEmpty && !l.startsWith(EventParser.HeaderPrefix))

    // ---- 3. Parse + validate ----------------------------------------------------------------
    val parsed: DStream[(String, Either[String, BookingEvent])] =
      lines.map(line => (line, EventParser.parse(line, refB.value))).cache()
    val valid: DStream[BookingEvent] =
      parsed.flatMap { case (_, Right(e)) => List(e); case _ => Nil }

    val batchNo        = new AtomicInteger(0)
    val totalInvalid   = new AtomicLong(0L)

    parsed.foreachRDD { rdd =>
      val n       = batchNo.incrementAndGet()
      val total   = rdd.count()
      val invalid = rdd.flatMap { case (line, Left(err)) => List(s"$err | $line"); case _ => Nil }.collect()
      totalInvalid.addAndGet(invalid.length.toLong)
      println()
      println(s"================ BATCH $n ================")
      println(s"[INGEST] received=$total valid=${total - invalid.length} invalid=${invalid.length} cumulativeInvalid=${totalInvalid.get()}")
      invalid.foreach(m => println(s"[REJECTED] $m"))
    }

    // ---- 4. Booking / cancellation state (Pair RDD keyed by booking_id) -----------------
    val bookingState: DStream[(String, BookingState)] =
      valid
        .map(e => (e.bookingId, e))
        .updateStateByKey[BookingState]((events: Seq[BookingEvent], prev: Option[BookingState]) =>
          BookingLogic.update(events, prev))

    bookingState.foreachRDD { rdd =>
      val states    = rdd.values
      val total     = states.count()
      val active    = states.filter(_.status == BookingStatus.Active).count()
      val cancelled = states.filter(_.status == BookingStatus.Cancelled).count()
      val orphans   = states.filter(_.status == BookingStatus.Orphan).count()
      val ignored   = states.map(_.ignoredEvents.toLong).fold(0L)(_ + _)
      println(s"[STATE] bookings=$total active=$active cancelled=$cancelled orphanCancels=$orphans ignoredDuplicateEvents=$ignored")
    }

    // ---- 5. Occupancy & availability (Pair RDD: (category, resource) -> units) ---------
    val occupancy: DStream[(Reports.OccKey, Reports.OccVal)] =
      bookingState
        .map { case (_, s) =>
          val isActive    = s.status == BookingStatus.Active
          val isCancelled = s.status == BookingStatus.Cancelled
          ((s.category, s.resourceId),
            (if (isActive) s.units else 0, if (isActive) 1 else 0, if (isCancelled) 1 else 0))
        }
        .reduceByKey(Reports.sum3 _)

    occupancy.foreachRDD { rdd =>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = Reports.occupancyRows(rdd, refB).toDF()
      Reports.runOccupancyReports(spark, df, cfg.output)
    }

    // ---- 6. Window operations (sliding window with inverse reduce) ------------------
    val windowed: DStream[(Reports.OccKey, WindowStats)] =
      valid
        .map(e => ((e.category, e.resourceId), WindowStats.of(e)))
        .reduceByKeyAndWindow(
          (a: WindowStats, b: WindowStats) => a + b,
          (a: WindowStats, b: WindowStats) => a - b,
          Seconds(cfg.windowSeconds),
          Seconds(cfg.slideSeconds),
          cfg.shufflePartitions,
          (kv: (Reports.OccKey, WindowStats)) => !kv._2.isEmpty)

    val windowLabel = s"${cfg.windowSeconds}s/${cfg.slideSeconds}s"
    windowed.foreachRDD { rdd =>
      if (rdd.isEmpty()) {
        println(s"[WINDOW $windowLabel] no events in the current window")
      } else {
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        Reports.runWindowReports(spark, Reports.windowRows(rdd, refB).toDF(), windowLabel)
      }
    }

    // ---- 7. Run ------------------------------------------------------------------------
    ssc.start()
    val runMillis: Long =
      if (cfg.runSeconds > 0) cfg.runSeconds * 1000L
      else if (replayBatches > 0) (replayBatches * cfg.batchSeconds + 1) * 1000L // stop right after the last batch
      else -1L

    if (runMillis > 0) {
      ssc.awaitTerminationOrTimeout(runMillis)
      ssc.stop(stopSparkContext = true, stopGracefully = false)
    } else {
      ssc.awaitTermination()
    }
  }

  /** Replay a CSV file as a stream: every `batchSize` lines become one micro-batch (queueStream). */
  private def replayStream(ssc: StreamingContext, cfg: AppConfig): (DStream[String], Int) = {
    val sc = ssc.sparkContext
    val all = sc.textFile(cfg.input).collect().toSeq
      .map(_.trim)
      .filter(l => l.nonEmpty && !l.startsWith(EventParser.HeaderPrefix))
    val groups = all.grouped(cfg.batchSize).toSeq
    val queue  = new mutable.Queue[RDD[String]]()
    groups.foreach(g => queue += sc.parallelize(g, 2))
    println(s"Replay source: ${all.size} events in ${groups.size} micro-batches of up to ${cfg.batchSize}")
    (ssc.queueStream(queue, oneAtATime = true), groups.size)
  }
}
