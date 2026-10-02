# Day28-Real-Time-Booking-Project

Real-time **hotel, flight and bus booking / cancellation** processing with **Scala + Apache Spark (Spark Streaming DStreams + Spark SQL)**.

The pipeline ingests booking events as a stream, keeps the **state of every booking**, computes **occupancy and availability** per resource using **Pair RDDs**, tracks recent activity with **sliding window operations**, enriches/validates events with **broadcast reference data**, and prints **Spark SQL reports** after every micro-batch.

> **Status note:** the code, unit tests and sample output were produced without a Scala/Spark toolchain available (offline environment). The sample output in this README / `docs/sample-output.txt` is derived from a reference simulation of the same logic on the sample data, **not** pasted from a real Spark run. After your first `sbt run`, replace it with your real console output / screenshots.

---

## 1. Architecture

```
 booking_events.csv (replay)  or  socket (nc -lk 9999)
              |
              v
   +--------------------------+       broadcast: resources.csv
   | parse + validate         |<----- (capacity, price, name, city)
   +--------------------------+
        | valid events                       | invalid -> [REJECTED] log
        |
        +--> (booking_id, event) --updateStateByKey--> booking state (ACTIVE / CANCELLED / ORPHAN_CANCEL)
        |                                                   |
        |                          ((category,resource), units) --reduceByKey--> occupancy & availability
        |                                                   |                         |
        |                                                   v                         v
        |                                              [STATE] summary      Spark SQL: reports 1-4
        |
        +--> ((category,resource), WindowStats) --reduceByKeyAndWindow(+, -)--> last 6s, slide 2s
                                                                                      |
                                                                                      v
                                                                         Spark SQL: window reports
```

## 2. Requirements covered

| Requirement | Where |
|---|---|
| Real-time hotel / flight / bus booking & cancellation events | `data/input/booking_events.csv` replayed as micro-batches (or a socket source) |
| Maintain booking / cancellation state | `BookingLogic.scala` + `updateStateByKey` keyed by `booking_id` |
| Occupancy & availability with **Pair RDD** | `(category, resourceId) -> (units, active, cancelled)` with `reduceByKey` |
| **Window operations** | `reduceByKeyAndWindow` with inverse function (window 6s, slide 2s) |
| **Broadcast** reference data | `resources.csv` broadcast; used for validation, capacity, price, name, city |
| **Spark SQL** reports | `Reports.scala` (temp views `occupancy`, `window_activity`) |
| Sample input / output | `data/`, `docs/sample-output.txt`, section 6 |
| Performance notes | section 8 |

## 3. Project structure

```
Day28-Real-Time-Booking-Project/
├── build.sbt
├── project/build.properties
├── .gitignore
├── README.md
├── data/
│   ├── input/booking_events.csv        # 40 sample events (incl. deliberately bad / duplicate / orphan ones)
│   └── reference/resources.csv         # 9 resources: capacity + unit price (broadcast)
├── docs/sample-output.txt              # full console output for the 5 micro-batches
├── scripts/push_to_github.sh
└── src
    ├── main/scala/com/day28/booking
    │   ├── BookingStreamingApp.scala   # DStream pipeline (entry point)
    │   ├── Models.scala                # case classes + CLI config
    │   ├── EventParser.scala           # validation using broadcast reference map
    │   ├── BookingLogic.scala          # pure booking state machine
    │   ├── ReferenceData.scala         # loads the reference file
    │   └── Reports.scala               # Pair RDD -> DataFrame, Spark SQL reports
    └── test/scala/com/day28/booking/BookingLogicSpec.scala
```

## 4. Data formats

**Event** - `event_id,event_time,booking_id,event_type,category,resource_id,units`

```
E001,2026-10-03T10:00:05,BK1001,BOOK,HOTEL,H101,3
E010,2026-10-03T10:00:50,BK1003,CANCEL,FLIGHT,F201,4
```

`CANCEL` releases the units of the original booking (taken from the stored state, not from the cancel event).

**Reference** - `resource_id,category,name,city,capacity,unit_price` (`H101,HOTEL,Taj Residency,Hyderabad,20,5000`).

Deliberate edge cases in the sample file:

| Event | Case | Expected handling |
|---|---|---|
| E015 | `HOLD` event type | rejected: unknown event_type |
| E016 | resource `H104` not in reference data | rejected: unknown resource_id |
| E024 | units = `x` | rejected: not an integer |
| E032 | units = `0` | rejected: units must be >= 1 |
| E021 | second `BOOK` for `BK1009` | ignored, counted in `ignoredDuplicateEvents` |
| E039 | second `CANCEL` for `BK1004` | ignored, counted in `ignoredDuplicateEvents` |
| E022 | `CANCEL` for unknown `BK1099` | state `ORPHAN_CANCEL`, no units released |
| E017 | pushes bus `B302` to 10 units on capacity 8 | status `OVERBOOKED` in batch 3 |

## 5. Run

Prerequisites: JDK 11/17, sbt 1.9+. (Spark is pulled in by sbt; no separate install needed.)

```bash
# unit tests (parser, state machine, window inverse, status thresholds)
sbt test

# replay the sample file: 8 events per 2-second micro-batch, window 6s / slide 2s
sbt run

# custom settings (all optional)
sbt "run --batch-size=8 --batch-seconds=2 --window-seconds=6 --slide-seconds=2 --run-seconds=20"
```

Live socket mode (two terminals):

```bash
nc -lk 9999                                   # terminal 1: paste event lines here
sbt "run --source=socket --port=9999"          # terminal 2
```

| Option | Default | Meaning |
|---|---|---|
| `--source` | `replay` | `replay` (file) or `socket` |
| `--input` / `--reference` | `data/...` | event file and reference file |
| `--batch-size` | 8 | events per micro-batch in replay mode |
| `--batch-seconds` | 2 | micro-batch interval |
| `--window-seconds` / `--slide-seconds` | 6 / 2 | must be multiples of the batch interval |
| `--run-seconds` | auto | replay stops ~1s after the last batch; increase it to watch the window drain |
| `--output` | `output` | occupancy snapshot CSV written after each batch (empty string disables) |
| `--checkpoint` | `checkpoint` | state/window checkpoint folder |
| `--master` | `local[2]` | ignored when started with `spark-submit --master` |

Delete the `checkpoint/` folder between runs. The replay source is a `queueStream`, which cannot be recovered from a checkpoint.

## 6. Sample output

Console markers: `[INGEST]` parse results, `[REJECTED]` bad records, `[STATE]` booking-state summary, `[REPORT n]` occupancy reports, `[WINDOW ...]` window reports.

**Batch 3** - the overbooking, sell-out and low-availability alerts appear:

```
================ BATCH 3 ================
[INGEST] received=8 valid=7 invalid=1 cumulativeInvalid=3
[REJECTED] units 'x' is not an integer | E024,2026-10-03T10:02:00,BK1019,BOOK,HOTEL,H101,x
[STATE] bookings=17 active=13 cancelled=3 orphanCancels=1 ignoredDuplicateEvents=1
[REPORT 1 - Occupancy & availability by category]
+--------+---------+--------+--------+---------+------------+--------------+-----------------+-------+
|category|resources|capacity|occupied|available|occupancyPct|activeBookings|cancelledBookings|revenue|
+--------+---------+--------+--------+---------+------------+--------------+-----------------+-------+
|BUS     |3        |28      |18      |12       |64.3        |5             |1                |14000.0|
|FLIGHT  |3        |30      |16      |14       |53.3        |4             |1                |88600.0|
|HOTEL   |3        |45      |17      |28       |37.8        |4             |1                |94000.0|
+--------+---------+--------+--------+---------+------------+--------------+-----------------+-------+
[REPORT 2 - Top 5 most occupied resources]
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
|category|resourceId|name           |capacity|occupiedUnits|availableUnits|occupancyPct|status          |
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
|BUS     |B302      |Express HYD-VJA|8       |10           |0             |125.0       |OVERBOOKED      |
|FLIGHT  |F203      |UK-880 DEL-BOM |8       |8            |0             |100.0       |SOLD_OUT        |
|HOTEL   |H103      |Sea View Resort|10      |9            |1             |90.0        |LOW_AVAILABILITY|
|FLIGHT  |F201      |AI-202 HYD-DEL |12      |6            |6             |50.0        |AVAILABLE       |
|BUS     |B301      |Volvo HYD-BLR  |10      |4            |6             |40.0        |AVAILABLE       |
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
[REPORT 3 - Availability alerts]
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
|category|resourceId|name           |capacity|occupiedUnits|availableUnits|occupancyPct|status          |
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
|BUS     |B302      |Express HYD-VJA|8       |10           |0             |125.0       |OVERBOOKED      |
|FLIGHT  |F203      |UK-880 DEL-BOM |8       |8            |0             |100.0       |SOLD_OUT        |
|HOTEL   |H103      |Sea View Resort|10      |9            |1             |90.0        |LOW_AVAILABILITY|
+--------+----------+---------------+--------+-------------+--------------+------------+----------------+
```

**Batch 5 (final)** - state after all 40 events:

```
================ BATCH 5 ================
[INGEST] received=8 valid=8 invalid=0 cumulativeInvalid=4
[STATE] bookings=24 active=13 cancelled=10 orphanCancels=1 ignoredDuplicateEvents=2
[REPORT 1 - Occupancy & availability by category]
+--------+---------+--------+--------+---------+------------+--------------+-----------------+--------+
|category|resources|capacity|occupied|available|occupancyPct|activeBookings|cancelledBookings|revenue |
+--------+---------+--------+--------+---------+------------+--------------+-----------------+--------+
|BUS     |3        |28      |24      |6        |85.7        |6             |3                |21000.0 |
|FLIGHT  |3        |30      |11      |19       |36.7        |2             |5                |51700.0 |
|HOTEL   |3        |45      |22      |23       |48.9        |5             |2                |111000.0|
+--------+---------+--------+--------+---------+------------+--------------+-----------------+--------+
[REPORT 2 - Top 5 most occupied resources]
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
|category|resourceId|name           |capacity|occupiedUnits|availableUnits|occupancyPct|status    |
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
|BUS     |B303      |Sleeper BLR-CHE|10      |12           |0             |120.0       |OVERBOOKED|
|BUS     |B301      |Volvo HYD-BLR  |10      |7            |3             |70.0        |AVAILABLE |
|BUS     |B302      |Express HYD-VJA|8       |5            |3             |62.5        |AVAILABLE |
|FLIGHT  |F202      |6E-512 BLR-HYD |10      |6            |4             |60.0        |AVAILABLE |
|HOTEL   |H101      |Taj Residency  |20      |12           |8             |60.0        |AVAILABLE |
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
[REPORT 3 - Availability alerts]
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
|category|resourceId|name           |capacity|occupiedUnits|availableUnits|occupancyPct|status    |
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
|BUS     |B303      |Sleeper BLR-CHE|10      |12           |0             |120.0       |OVERBOOKED|
+--------+----------+---------------+--------+-------------+--------------+------------+----------+
[REPORT 4 - Revenue by city]
+---------+-------------+--------+
|city     |occupiedUnits|revenue |
+---------+-------------+--------+
|Hyderabad|29           |103900.0|
|Bengaluru|18           |28800.0 |
|Mumbai   |6            |27000.0 |
|Goa      |4            |24000.0 |
|Delhi    |0            |0.0     |
+---------+-------------+--------+
[WINDOW 6s/2s - Activity by category]
+--------+--------+-------------+-----------+--------------+--------+-------------+
|category|bookings|cancellations|bookedUnits|cancelledUnits|netUnits|cancelRatePct|
+--------+--------+-------------+-----------+--------------+--------+-------------+
|BUS     |5       |4            |21         |21            |0       |80.0         |
|FLIGHT  |3       |5            |16         |18            |-2      |166.7        |
|HOTEL   |4       |1            |20         |5             |15      |25.0         |
+--------+--------+-------------+-----------+--------------+--------+-------------+
[WINDOW 6s/2s - Hot resources (top 3 by booked units)]
+--------+----------+---------------+--------+-----------+
|category|resourceId|name           |bookings|bookedUnits|
+--------+----------+---------------+--------+-----------+
|BUS     |B301      |Volvo HYD-BLR  |2       |11         |
|HOTEL   |H101      |Taj Residency  |2       |9          |
|BUS     |B303      |Sleeper BLR-CHE|2       |8          |
+--------+----------+---------------+--------+-----------+
```

Full output for all five batches: [`docs/sample-output.txt`](docs/sample-output.txt). The occupancy snapshot is also written to `output/occupancy_snapshot/` as CSV after every batch.

### Reading the results

* **Occupancy is derived from state, not from raw events.** Cancelling `BK1004` twice, or booking `BK1009` twice, does not distort availability.
* **Window numbers are event activity, not truth.** In the window report, FLIGHT shows more cancellations than bookings because cancels of bookings made *before* the window are still cancel events inside it. The orphan cancel `E022` is counted in the window but released no units in the state.
* **Overbooking is reported, not blocked.** `B303` ends at 12 units on capacity 10 (`OVERBOOKED`). Rejecting a booking at ingestion would need a transactional inventory store, which is out of scope for this exercise.

## 7. Concepts used

* **Pair RDDs:** `(booking_id, event)`, `((category, resource), (units, active, cancelled))`, `((category, resource), WindowStats)`; `reduceByKey`, `updateStateByKey`, `reduceByKeyAndWindow`.
* **Stateful streaming:** `updateStateByKey` + checkpointing; the state function is a pure Scala function (`BookingLogic.update`) so it is unit-tested without Spark.
* **Window operations:** window 6s / slide 2s over 2s batches = the last 3 micro-batches. `WindowStats` is additive and subtractable, so Spark can use the *inverse reduce* variant.
* **Broadcast variable:** one read-only `Map[resourceId, Resource]` per executor instead of joining a stream with a reference dataset.
* **Spark SQL:** each batch's RDD becomes a DataFrame, is registered as a temp view and queried with SQL (`GROUP BY`, `CASE`-style status, `NULLIF`, `ORDER BY ... LIMIT`).
* **Transformations vs actions:** `map / flatMap / filter / reduceByKey / updateStateByKey / reduceByKeyAndWindow` are lazy; `count / collect / show / write` inside `foreachRDD` trigger the jobs.

## 8. Performance notes

1. **Broadcast, not join.** Reference data is tiny, so broadcasting avoids a shuffle per batch. Rule of thumb: broadcast when the table fits comfortably in executor memory (tens of MB).
2. **Batch interval vs processing time.** Processing time per batch must stay below the batch interval, otherwise scheduling delay grows without bound. Watch *Streaming* tab -> *Processing Time* in the Spark UI (`http://localhost:4040`). The reports (several Spark SQL jobs per batch) are the dominant cost in this demo, so on a slow machine raise `--batch-seconds` (and keep window/slide multiples).
3. **Inverse-reduce window.** `reduceByKeyAndWindow(+, -)` updates the window by adding the newest batch and subtracting the oldest, instead of re-reducing all batches. The `filterFunc` drops keys whose stats return to zero so the window state does not grow forever. This requires checkpointing.
4. **Cache the parsed stream.** `parsed` feeds several consumers (ingest log, valid events); `.cache()` avoids re-parsing the same RDD.
5. **Shuffle partitions.** Set to `4` for local mode (`--shuffle-partitions`); the Spark default of 200 would create hundreds of near-empty tasks per batch.
6. **State growth.** `updateStateByKey` keeps *every* booking forever and rescans *all* state every batch, so batch time grows with the number of bookings. For production use `mapWithState` (or Structured Streaming `flatMapGroupsWithState`) with a timeout to evict old `CANCELLED` bookings, or archive them to storage.
7. **Occupancy recomputation.** Occupancy is re-aggregated from the full state each batch (simple and always consistent). At large scale, maintain per-resource deltas incrementally instead.
8. **Driver-side output.** `collect()` is used only for small rejected-record samples; large result sets should be written with `df.write` rather than collected.
9. **Small files.** The snapshot is written with `coalesce(1)` and overwritten every batch, which is fine for a demo; a real sink should append partitioned Parquet and compact periodically.
10. **Delivery semantics.** Output operations can re-run after a failure (at-least-once), so side effects such as the CSV snapshot must be idempotent (they are: overwrite). Use a replayable source (Kafka with offset tracking) for end-to-end guarantees.
11. **Technology choice.** DStreams are in maintenance mode; they are used here because they match the Pair-RDD / window-operation focus of this exercise. The same design maps to Structured Streaming (`groupBy(window(...))`, `flatMapGroupsWithState`, `broadcast()` join).

## 9. Push to GitHub

```bash
# 1. create an EMPTY repo named Day28-Real-Time-Booking-Project on github.com (no README), then:
cd Day28-Real-Time-Booking-Project
git init -b main            # skip if the folder is already a git repo
git add .
git commit -m "Day 28: Real-time booking project (Scala + Spark Streaming, Pair RDD, windows, broadcast, Spark SQL)"
git remote add origin https://github.com/<your-username>/Day28-Real-Time-Booking-Project.git
git push -u origin main
```

or `bash scripts/push_to_github.sh <your-username>` (see the script).
