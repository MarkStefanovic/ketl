package ketl.domain

import java.time.DayOfWeek
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DslMarker annotation class ScheduleDSL

@ExperimentalTime
fun daily(
  displayName: String,
  init: DaySchedule.Builder.() -> Unit = {},
): Schedule {
  val parts = DaySchedule.Builder(
    startWeekday = DayOfWeek.MONDAY,
    endWeekday = DayOfWeek.SUNDAY,
  ).apply(init).build().parts
  return Schedule(
    displayName = displayName,
    parts = parts,
  )
}

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
  init: WeekSchedule.Builder.() -> Unit = {},
): Schedule {
  val parts = WeekSchedule.Builder().apply(init).build().scheduleParts
  return Schedule(displayName = displayName, parts = parts)
}

@ExperimentalTime
data class WeekSchedule(val scheduleParts: Set<SchedulePart>) {
  @ScheduleDSL
  class Builder {
    private val schedParts = mutableListOf<SchedulePart>()

    fun build() = WeekSchedule(schedParts.toSet())

    fun between(
      start: DayOfWeek = DayOfWeek.MONDAY, // monday = 1
      end: DayOfWeek = DayOfWeek.SUNDAY, // sunday = 7
      init: DaySchedule.Builder.() -> Unit = {},
    ) {
      require(start.value <= end.value) {
        "The starting weekday must be on or before the ending weekday, starting from Monday as 1, and Sunday as 7."
      }
      schedParts.addAll(
        DaySchedule.Builder(
          startWeekday = start,
          endWeekday = end,
        )
          .apply(init)
          .build()
          .parts
      )
    }
  }
}

@ExperimentalTime
data class DaySchedule(val parts: Set<SchedulePart>) {
  @ScheduleDSL
  class Builder(
    private val startWeekday: DayOfWeek,
    private val endWeekday: DayOfWeek,
  ) {
    private val scheduleParts = mutableListOf<SchedulePart>()

    fun build(): DaySchedule = DaySchedule(scheduleParts.toSet())

    fun every(
      frequency: Duration,
      starting: LocalDateTime = LocalDateTime.MIN,
    ) {
      val win =
        ExecutionWindow(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
        )
      scheduleParts.add(
        SchedulePart(
          frequency = frequency,
          window = win,
          startDateTime = starting,
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
      scheduleParts.addAll(
        Every.Builder(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
          startHour = startHour,
          endHour = endHour,
        )
          .apply(init)
          .build()
          .parts
      )
    }
  }
}

@ExperimentalTime
data class Every(val parts: Set<SchedulePart>) {
  @ScheduleDSL
  class Builder(
    private val startWeekday: DayOfWeek,
    private val endWeekday: DayOfWeek,
    private val startHour: Int,
    private val endHour: Int,
  ) {
    private val scheduleParts = mutableListOf<SchedulePart>()

    fun build(): Every =
      if (scheduleParts.isEmpty()) {
        throw Exception("No frequency was provided.")
      } else {
        Every(scheduleParts.toSet())
      }

    fun every(
      frequency: Duration,
      starting: LocalDateTime = LocalDateTime.MIN,
    ) {
      val win =
        ExecutionWindow(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
          startHour = startHour,
          endHour = endHour,
        )
      scheduleParts.add(
        SchedulePart(
          frequency = frequency,
          window = win,
          startDateTime = starting,
        )
      )
    }
  }
}
