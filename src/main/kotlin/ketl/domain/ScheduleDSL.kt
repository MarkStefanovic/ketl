package ketl.domain

import java.time.DayOfWeek
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DslMarker annotation class ScheduleDSL

@ExperimentalTime
fun every(frequency: Duration, init: Every.Builder.() -> Unit): List<Schedule> =
  Every.Builder(frequency).apply(init).build().schedule

@ExperimentalTime
data class Every(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(private val frequency: Duration) {
    private val sched = mutableListOf<Schedule>()

    fun build() = Every(sched)

    fun onWeekdays(init: WeeklySchedule.Builder.() -> Unit = {}) {
      sched.addAll(WeeklySchedule.Builder(frequency).apply(init).build().schedule)
    }
  }
}

@ExperimentalTime
data class WeeklySchedule(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(private val frequency: Duration) {
    private val sched = mutableListOf<Schedule>()

    fun build() = WeeklySchedule(sched)

    fun between(
      startWeekday: DayOfWeek = DayOfWeek.MONDAY, // monday = 1
      endWeekday: DayOfWeek = DayOfWeek.SUNDAY, // sunday = 7
      init: WeekdaySchedule.Builder.() -> Unit = {},
    ) {
      sched.addAll(
        WeekdaySchedule.Builder(
          frequency = frequency,
          startWeekday = startWeekday,
          endWeekday = endWeekday,
        )
          .apply(init)
          .build()
          .schedule
      )
    }
  }
}

@ExperimentalTime
data class WeekdaySchedule(val schedule: List<Schedule>) {
  @ScheduleDSL
  class Builder(
    private val frequency: Duration,
    private val startWeekday: DayOfWeek,
    private val endWeekday: DayOfWeek,
  ) {
    private val wins = mutableListOf<ExecutionWindow>()

    fun build() = WeekdaySchedule(wins.map { w -> Schedule(frequency = frequency, window = w) })

    fun betweenTheHoursOf(startHour: Int, endHour: Int) {
      wins.add(
        ExecutionWindow(
          startWeekday = startWeekday,
          endWeekday = endWeekday,
          startHour = startHour,
          endHour = endHour,
        )
      )
    }
  }
}
