package ketl.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JobTest {
  @Test
  fun isReady_happy_path() {
    val log = LogMessages("test")

    val job = Job(
      name = "Test Job",
      schedule = listOf(
        Schedule(Duration.seconds(10)),
      ),
      timeout = Duration.seconds(10),
      retries = 0,
      ctx = BaseContext(log),
    ) {
      success()
    }
    val isReady = job.isReady(
      refTime = LocalDate.of(2021, 8, 14).atStartOfDay(),
      lastRun = null,
    )
    assert(isReady)
  }
}
