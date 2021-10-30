package ketl.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScheduleTest {
  @Test
  fun ready_happy_path() {
    val schedule =
      Schedule(
        displayName = "test schedule",
        parts = setOf(SchedulePart(frequency = Duration.seconds(10))),
      )
    val isReady =
      schedule.ready(
        refTime = LocalDateTime.of(2010, 1, 1, 1, 1, 1),
        lastRun = null,
      )
    assert(isReady)
  }

  @Test
  fun durationHasElapsed_happy_path() {
    val ready =
      durationHasElapsed(
        duration = Duration.seconds(10),
        lastRun = null,
        refDateTime = LocalDateTime.now(),
      )
    assert(ready)
  }
}
