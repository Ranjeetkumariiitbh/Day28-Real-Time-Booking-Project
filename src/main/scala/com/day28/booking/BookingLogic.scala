package com.day28.booking

/**
 * Pure state-transition logic (no Spark types) so it can be unit-tested directly.
 *
 * State machine per booking_id:
 *   (none)     + BOOK    -> ACTIVE
 *   (none)     + CANCEL  -> ORPHAN_CANCEL      (cancel arrived before / without its booking)
 *   ORPHAN     + BOOK    -> CANCELLED          (late booking, already cancelled)
 *   ACTIVE     + CANCEL  -> CANCELLED
 *   anything else        -> ignored (duplicate BOOK / duplicate CANCEL), counted in ignoredEvents
 */
object BookingLogic {

  def update(events: Seq[BookingEvent], prev: Option[BookingState]): Option[BookingState] =
    if (events.isEmpty) prev
    else events.sortBy(e => (e.eventTime, e.eventId)).foldLeft(prev)(step)

  private def step(state: Option[BookingState], e: BookingEvent): Option[BookingState] =
    (state, e.eventType) match {
      case (None, EventType.Book) =>
        Some(BookingState(e.bookingId, e.category, e.resourceId, e.units, BookingStatus.Active, 0, e.eventTime))

      case (None, EventType.Cancel) =>
        Some(BookingState(e.bookingId, e.category, e.resourceId, 0, BookingStatus.Orphan, 0, e.eventTime))

      case (Some(s), EventType.Book) if s.status == BookingStatus.Orphan =>
        Some(s.copy(units = e.units, status = BookingStatus.Cancelled, lastEventTime = e.eventTime))

      case (Some(s), EventType.Cancel) if s.status == BookingStatus.Active =>
        Some(s.copy(status = BookingStatus.Cancelled, lastEventTime = e.eventTime))

      case (Some(s), _) =>
        Some(s.copy(ignoredEvents = s.ignoredEvents + 1))

      case (None, _) => None // unreachable: parser only emits BOOK / CANCEL
    }
}
