package ketl.domain

import java.time.DayOfWeek
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DslMarker annotation class ScheduleDSL

@ExperimentalTime
fun daily(
  displayName: String,
  startDateTime: LocalDateTime = LocalDateTime.MIN,
  init: DaySchedule.Builder.() -> Unit = {},
) =
  DaySchedule.Builder(
    displayName = displayName,
    startDateTime = startDateTime,
    startWeekday = DayOfWeek.MONDAY,
    endWeekday = DayOfWeek.SUNDAY,
  )
    .apply(init)
    .build()
    .schedule

@ExperimentalTime
fun every(
  displayName: String,
  frequency: Duration,
  startDateTime: LocalDateTime = LocalDateTime.MIN,
): Schedule =
  Schedule(
    displayName = displayName,
    parts = setOf(
      SchedulePart(
        frequency = frequency,
        window = ExecutionWindow.ANYTIME,
        startDateTime = startDateTime,
      )
    ),
  )

@ExperimentalTime
fun weekly(
  displayName: String,
  startDateTime: LocalDateTime = LocalDateTime.MIN,
  init: WeekSchedule.Builder.() -> Unit = {},
): List<Schedule> = WeekSchedule.Builder(
  displayName = displayName,
  startDateTime = startDateTime
).apply(init).build().schedule

@ExperimentalTime
data class WeekSchedule(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(
    private val displayName: String,
    private val startDateTime: LocalDateTime,
  ) {
    private val sched = mutableListOf<Schedule>()

    fun build() = WeekSchedule(sched)

    fun between(
      start: DayOfWeek = DayOfWeek.MONDAY, // monday = 1
      end: DayOfWeek = DayOfWeek.SUNDAY, // sunday = 7
      init: DaySchedule.Builder.() -> Unit = {},
    ) {
      require(start.value <= end.value) {
        "The starting weekday must be on or before the ending weekday, starting from Monday as 1, and Sunday as 7."
      }
      sched.addAll(
        DaySchedule.Builder(
          displayName = displayName,
          startDateTime = startDateTime,
          startWeekday = start,
          endWeekday = end,
        )
          .apply(init)
          .build()
          .schedule
      )
    }
  }
}

@ExperimentalTime
data class DaySchedule(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(
    private val displayName: String,
    private val startDateTime: LocalDateTime,
    private val startWeekday: DayOfWeek,
    private val endWeekday: DayOfWeek,
  ) {
    private val sched = mutableListOf<Schedule>()

    fun build(): DaySchedule = DaySchedule(sched)

    fun every(frequency: Duration) {
      val win =
        ExecutionWindow(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
        )
      sched.add(
        Schedule(
          displayName = displayName,
          parts = setOf(
            SchedulePart(
              frequency = frequency,
              window = win,
              startDateTime = startDateTime,
            ),
          ),
        )
      )
    }

    fun between(
      startHour: Int,
      endHour: Int,
      init: Every.Builder.() -> Unit,
    ) {
      require(startHour <= endHour) {
        "The starting hour must be on or before the ending hour."
      }
      sched.addAll(
        Every.Builder(
          displayName = displayName,
          startDateTime = startDateTime,
          startWeekday = startWeekday,
          endWeekday = endWeekday,
          startHour = startHour,
          endHour = endHour,
        )
          .apply(init)
          .build()
          .schedule
      )
    }
  }
}

@ExperimentalTime
data class Every(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(
    private val displayName: String,
    private val startDateTime: LocalDateTime,
    private val startWeekday: DayOfWeek,
    private val endWeekday: DayOfWeek,
    private val startHour: Int,
    private val endHour: Int,
  ) {
    private val sched = mutableListOf<Schedule>()

    fun build(): Every =
      if (sched.isEmpty()) {
        throw Exception("No frequency was provided.")
      } else {
        Every(sched)
      }

    fun every(frequency: Duration) {
      val win =
        ExecutionWindow(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
          startHour = startHour,
          endHour = endHour,
        )
      sched.add(
        Schedule(
          displayName = displayName,
          parts = setOf(
            SchedulePart(
              frequency = frequency,
              window = win,
              startDateTime = startDateTime,
            )
          ),
        )
      )
    }
  }
}
