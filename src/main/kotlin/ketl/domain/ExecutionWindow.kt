package ketl.domain

import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.Month

data class ExecutionWindow(
  val startMonth: Month = Month.JANUARY,
  val endMonth: Month = Month.DECEMBER,
  val startMonthday: Int = 1,
  val endMonthday: Int = 31,
  val startWeekday: DayOfWeek = DayOfWeek.MONDAY, // monday = 1
  val endWeekday: DayOfWeek = DayOfWeek.SUNDAY, // sunday = 7
  val startHour: Int = 0,
  val endHour: Int = 23,
  val startMinute: Int = 0,
  val endMinute: Int = 59,
  val startSecond: Int = 0,
  val endSecond: Int = 59,
) {
  init {
    require(startMonthday in 1..31) { "startMonthday must be between 1 and 31." }
    require(endMonthday in 1..31) { "endMonthday must be between 1 and 31." }

    require(startHour in 0..23) { "startHour must be between 0 and 23." }
    require(endHour in 0..23) { "endHour must be between 0 and 23." }

    require(startMinute in 0..59) { "startMinute must be between 0 and 59." }
    require(endMinute in 0..59) { "endMinute must be between 0 and 59." }

    require(startSecond in 0..59) { "startSecond must be between 0 and 59." }
    require(endSecond in 0..59) { "endSecond must be between 0 and 59." }
  }

  fun inWindow(refTime: LocalDateTime): Boolean =
    (refTime.monthValue in startMonth.value..endMonth.value) &&
      (refTime.dayOfMonth in startMonthday..endMonthday) &&
      (refTime.dayOfWeek.value in startWeekday.value..endWeekday.value) &&
      (refTime.hour in startHour..endHour) &&
      (refTime.minute in startMinute..endMinute) &&
      (refTime.second in startSecond..endSecond)

  companion object {
    val ANYTIME = ExecutionWindow()
  }
}
