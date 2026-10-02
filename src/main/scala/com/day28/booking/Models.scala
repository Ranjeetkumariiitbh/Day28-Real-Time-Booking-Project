package com.day28.booking

object EventType {
  val Book   = "BOOK"
  val Cancel = "CANCEL"
}

object BookingStatus {
  val Active    = "ACTIVE"
  val Cancelled = "CANCELLED"
  /** A CANCEL arrived for a booking we have never seen (out-of-order / lost BOOK). */
  val Orphan    = "ORPHAN_CANCEL"
}

/** Reference (master) data row - broadcast to every executor. */
final case class Resource(
  resourceId: String,
  category: String,
  name: String,
  city: String,
  capacity: Int,
  unitPrice: Double
)

/** One validated booking or cancellation event. */
final case class BookingEvent(
  eventId: String,
  eventTime: String,
  bookingId: String,
  eventType: String,
  category: String,
  resourceId: String,
  units: Int
)

/** Per-booking state kept across micro-batches (updateStateByKey). */
final case class BookingState(
  bookingId: String,
  category: String,
  resourceId: String,
  units: Int,
  status: String,
  ignoredEvents: Int,
  lastEventTime: String
)

/** Additive statistics so the window can use an inverse function (incremental sliding window). */
final case class WindowStats(bookings: Int, cancellations: Int, bookedUnits: Int, cancelledUnits: Int) {
  def +(o: WindowStats): WindowStats =
    WindowStats(bookings + o.bookings, cancellations + o.cancellations,
      bookedUnits + o.bookedUnits, cancelledUnits + o.cancelledUnits)
  def -(o: WindowStats): WindowStats =
    WindowStats(bookings - o.bookings, cancellations - o.cancellations,
      bookedUnits - o.bookedUnits, cancelledUnits - o.cancelledUnits)
  def isEmpty: Boolean =
    bookings == 0 && cancellations == 0 && bookedUnits == 0 && cancelledUnits == 0
}

object WindowStats {
  def of(e: BookingEvent): WindowStats =
    if (e.eventType == EventType.Book) WindowStats(1, 0, e.units, 0)
    else WindowStats(0, 1, 0, e.units)
}

final case class OccupancyRow(
  category: String,
  resourceId: String,
  name: String,
  city: String,
  capacity: Int,
  occupiedUnits: Int,
  availableUnits: Int,
  occupancyPct: Double,
  activeBookings: Int,
  cancelledBookings: Int,
  revenue: Double,
  status: String
)

final case class WindowRow(
  category: String,
  resourceId: String,
  name: String,
  city: String,
  bookings: Int,
  cancellations: Int,
  bookedUnits: Int,
  cancelledUnits: Int
)

final case class AppConfig(
  source: String = "replay",          // replay | socket
  input: String = "data/input/booking_events.csv",
  reference: String = "data/reference/resources.csv",
  output: String = "output",
  checkpoint: String = "checkpoint",
  master: String = "local[2]",
  host: String = "localhost",
  port: Int = 9999,
  batchSize: Int = 8,                 // events per micro-batch in replay mode
  batchSeconds: Int = 2,
  windowSeconds: Int = 6,
  slideSeconds: Int = 2,
  shufflePartitions: Int = 4,
  runSeconds: Int = -1                // -1 = auto in replay mode, run forever in socket mode
)

object AppConfig {
  /** Tiny `--key=value` parser (no external dependency). */
  def parse(args: Array[String]): AppConfig = {
    val kv = args.collect { case a if a.startsWith("--") && a.contains("=") =>
      val i = a.indexOf('=')
      a.substring(2, i) -> a.substring(i + 1)
    }.toMap
    val d = AppConfig()
    val cfg = AppConfig(
      source            = kv.getOrElse("source", d.source),
      input             = kv.getOrElse("input", d.input),
      reference         = kv.getOrElse("reference", d.reference),
      output            = kv.getOrElse("output", d.output),
      checkpoint        = kv.getOrElse("checkpoint", d.checkpoint),
      master            = kv.getOrElse("master", d.master),
      host              = kv.getOrElse("host", d.host),
      port              = kv.get("port").map(_.toInt).getOrElse(d.port),
      batchSize         = kv.get("batch-size").map(_.toInt).getOrElse(d.batchSize),
      batchSeconds      = kv.get("batch-seconds").map(_.toInt).getOrElse(d.batchSeconds),
      windowSeconds     = kv.get("window-seconds").map(_.toInt).getOrElse(d.windowSeconds),
      slideSeconds      = kv.get("slide-seconds").map(_.toInt).getOrElse(d.slideSeconds),
      shufflePartitions = kv.get("shuffle-partitions").map(_.toInt).getOrElse(d.shufflePartitions),
      runSeconds        = kv.get("run-seconds").map(_.toInt).getOrElse(d.runSeconds)
    )
    require(cfg.windowSeconds % cfg.batchSeconds == 0, "window-seconds must be a multiple of batch-seconds")
    require(cfg.slideSeconds % cfg.batchSeconds == 0, "slide-seconds must be a multiple of batch-seconds")
    cfg
  }
}
