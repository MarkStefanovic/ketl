package ketl.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.DayOfWeek
import java.time.LocalDateTime
import java.time.Month
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScheduleDSLTest {
  @Test
  fun everyday_onHoursBetween_happy_path() {
    val sched = daily("test schedule") {
      between(0, 12) {
        every(Duration.minutes(5))
      }
    }
    assertEquals(
      expected = listOf(
        Schedule(
          displayName = "test schedule",
          parts = setOf(
            SchedulePart(
              frequency = Duration.minutes(5),
              window = ExecutionWindow(
                startMonth = Month.JANUARY,
                endMonth = Month.DECEMBER,
                startMonthday = 1,
                endMonthday = 31,
                startWeekday = DayOfWeek.MONDAY,
                endWeekday = DayOfWeek.SUNDAY,
                startHour = 0,
                endHour = 12,
                startMinute = 0,
                endMinute = 59,
                startSecond = 0,
                endSecond = 59
              ),
              startDateTime = LocalDateTime.MIN,
            )
          ),
        ),
      ),
      actual = sched,
    )
  }

  @Test
  fun weekly_onHoursBetween_happy_path() {
    val sched = weekly("test schedule") {
      between(DayOfWeek.MONDAY, DayOfWeek.SATURDAY) {
        between(0, 12) {
          every(Duration.minutes(5))
        }
        between(20, 23) {
          every(Duration.minutes(10))
        }
      }
    }
    assertEquals(
      expected = listOf(
        Schedule(
          displayName = "test schedule",
          parts = setOf(
            SchedulePart(
              frequency = Duration.minutes(5),
              window = ExecutionWindow(
                startMonth = Month.JANUARY,
                endMonth = Month.DECEMBER,
                startMonthday = 1,
                endMonthday = 31,
                startWeekday = DayOfWeek.MONDAY,
                endWeekday = DayOfWeek.SATURDAY,
                startHour = 0,
                endHour = 12,
                startMinute = 0,
                endMinute = 59,
                startSecond = 0,
                endSecond = 59
              ),
              startDateTime = LocalDateTime.MIN,
            )
          ),
        ),
        Schedule(
          displayName = "test schedule",
          parts = setOf(
            SchedulePart(
              frequency = Duration.minutes(10),
              window = ExecutionWindow(
                startMonth = Month.JANUARY,
                endMonth = Month.DECEMBER,
                startMonthday = 1,
                endMonthday = 31,
                startWeekday = DayOfWeek.MONDAY,
                endWeekday = DayOfWeek.SATURDAY,
                startHour = 20,
                endHour = 23,
                startMinute = 0,
                endMinute = 59,
                startSecond = 0,
                endSecond = 59
              ),
              startDateTime = LocalDateTime.MIN,
            ),
          )
        ),
      ),
      actual = sched,
    )
  }
}
