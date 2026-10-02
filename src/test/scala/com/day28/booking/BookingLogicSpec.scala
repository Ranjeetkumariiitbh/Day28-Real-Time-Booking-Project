package com.day28.booking

import org.scalatest.funsuite.AnyFunSuite

class BookingLogicSpec extends AnyFunSuite {

  private val ref = Map(
    "H101" -> Resource("H101", "HOTEL", "Taj Residency", "Hyderabad", 20, 5000.0),
    "B301" -> Resource("B301", "BUS", "Volvo HYD-BLR", "Hyderabad", 10, 1200.0)
  )

  private def ev(id: String, t: String, b: String, ty: String, units: Int = 2) =
    BookingEvent(id, s"2026-10-03T10:00:$t", b, ty, "HOTEL", "H101", units)

  // ---------- parser ----------
  test("parser accepts a valid BOOK line") {
    val r = EventParser.parse("E1,2026-10-03T10:00:05,BK1,book,hotel,H101,3", ref)
    assert(r == Right(BookingEvent("E1", "2026-10-03T10:00:05", "BK1", "BOOK", "HOTEL", "H101", 3)))
  }

  test("parser rejects bad records with a reason") {
    def err(l: String) = EventParser.parse(l, ref).left.getOrElse("")
    assert(err("E1,2026-10-03T10:00:05,BK1,HOLD,HOTEL,H101,2").contains("unknown event_type"))
    assert(err("E1,2026-10-03T10:00:05,BK1,BOOK,HOTEL,H999,2").contains("unknown resource_id"))
    assert(err("E1,2026-10-03T10:00:05,BK1,BOOK,BUS,H101,2").contains("not a BUS"))
    assert(err("E1,2026-10-03T10:00:05,BK1,BOOK,HOTEL,H101,x").contains("not an integer"))
    assert(err("E1,2026-10-03T10:00:05,BK1,BOOK,HOTEL,H101,0").contains("units must be >= 1"))
    assert(err("E1,not-a-time,BK1,BOOK,HOTEL,H101,2").contains("bad event_time"))
    assert(err("E1,2026-10-03T10:00:05,BK1,BOOK").contains("expected 7 fields"))
  }

  // ---------- state machine ----------
  test("BOOK creates an ACTIVE booking") {
    val s = BookingLogic.update(Seq(ev("E1", "01", "BK1", "BOOK", 3)), None).get
    assert(s.status == BookingStatus.Active && s.units == 3 && s.ignoredEvents == 0)
  }

  test("CANCEL after BOOK releases the booking") {
    val s1 = BookingLogic.update(Seq(ev("E1", "01", "BK1", "BOOK", 3)), None)
    val s2 = BookingLogic.update(Seq(ev("E2", "02", "BK1", "CANCEL")), s1).get
    assert(s2.status == BookingStatus.Cancelled && s2.units == 3)
  }

  test("BOOK and CANCEL in the same batch are applied in event-time order") {
    val s = BookingLogic.update(Seq(ev("E2", "02", "BK1", "CANCEL"), ev("E1", "01", "BK1", "BOOK", 4)), None).get
    assert(s.status == BookingStatus.Cancelled && s.units == 4)
  }

  test("duplicate BOOK and duplicate CANCEL are ignored but counted") {
    val s1 = BookingLogic.update(Seq(ev("E1", "01", "BK1", "BOOK", 3)), None)
    val s2 = BookingLogic.update(Seq(ev("E2", "02", "BK1", "BOOK", 3)), s1).get
    assert(s2.status == BookingStatus.Active && s2.ignoredEvents == 1)
    val s3 = BookingLogic.update(Seq(ev("E3", "03", "BK1", "CANCEL"), ev("E4", "04", "BK1", "CANCEL")), Some(s2)).get
    assert(s3.status == BookingStatus.Cancelled && s3.ignoredEvents == 2)
  }

  test("CANCEL without a booking becomes ORPHAN_CANCEL; a late BOOK ends as CANCELLED") {
    val s1 = BookingLogic.update(Seq(ev("E1", "01", "BK9", "CANCEL")), None).get
    assert(s1.status == BookingStatus.Orphan && s1.units == 0)
    val s2 = BookingLogic.update(Seq(ev("E2", "02", "BK9", "BOOK", 5)), Some(s1)).get
    assert(s2.status == BookingStatus.Cancelled && s2.units == 5)
  }

  test("no events keeps previous state untouched") {
    val s1 = BookingLogic.update(Seq(ev("E1", "01", "BK1", "BOOK")), None)
    assert(BookingLogic.update(Seq.empty, s1) == s1)
  }

  // ---------- window stats (inverse function must undo the forward function) ----------
  test("WindowStats subtract is the inverse of add") {
    val a = WindowStats.of(ev("E1", "01", "BK1", "BOOK", 3))
    val b = WindowStats.of(ev("E2", "02", "BK2", "CANCEL", 2))
    assert(((a + b) - b) == a)
    assert((a - a).isEmpty)
  }

  // ---------- availability status ----------
  test("status thresholds") {
    assert(Reports.status(10, 11, 110.0) == "OVERBOOKED")
    assert(Reports.status(10, 10, 100.0) == "SOLD_OUT")
    assert(Reports.status(10, 8, 80.0) == "LOW_AVAILABILITY")
    assert(Reports.status(10, 7, 70.0) == "AVAILABLE")
  }
}
