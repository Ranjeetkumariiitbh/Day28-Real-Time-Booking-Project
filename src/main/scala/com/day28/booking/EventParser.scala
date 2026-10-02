package com.day28.booking

import scala.util.{Failure, Success, Try}

object EventParser {

  val HeaderPrefix = "event_id"
  private val Categories = Set("HOTEL", "FLIGHT", "BUS")

  /**
   * Parse and validate one CSV line:
   * event_id,event_time,booking_id,event_type,category,resource_id,units
   *
   * Validation uses the broadcast reference map, so an unknown resource or a
   * category/resource mismatch is rejected without any shuffle or join.
   */
  def parse(line: String, ref: Map[String, Resource]): Either[String, BookingEvent] = {
    val p = line.split(",", -1).map(_.trim)
    if (p.length != 7) {
      Left(s"expected 7 fields but got ${p.length}")
    } else {
      val eventId   = p(0)
      val eventTime = p(1)
      val bookingId = p(2)
      val eventType = p(3).toUpperCase
      val category  = p(4).toUpperCase
      val resource  = p(5)
      val rawUnits  = p(6)

      if (eventId.isEmpty || bookingId.isEmpty) Left("missing event_id or booking_id")
      else if (Try(java.time.LocalDateTime.parse(eventTime)).isFailure) Left(s"bad event_time '$eventTime'")
      else if (eventType != EventType.Book && eventType != EventType.Cancel) Left(s"unknown event_type '${p(3)}'")
      else if (!Categories.contains(category)) Left(s"unknown category '${p(4)}'")
      else ref.get(resource) match {
        case None => Left(s"unknown resource_id '$resource'")
        case Some(r) if r.category != category => Left(s"resource $resource is not a $category resource")
        case Some(_) =>
          Try(rawUnits.toInt) match {
            case Success(u) if u >= 1 => Right(BookingEvent(eventId, eventTime, bookingId, eventType, category, resource, u))
            case Success(u)           => Left(s"units must be >= 1 but was $u")
            case Failure(_)           => Left(s"units '$rawUnits' is not an integer")
          }
      }
    }
  }
}
