package ketl.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

private const val expectedToStringResult = """ETLJob [
  name: Test Job
  schedule: test schedule
  timeout: 10s
  retries: 0
  dependencies: []
]"""

class DummyContext : JobContext() {
  override fun close() {}
}

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ETLJobTest {
  @Test
  fun isReady_happy_path() {
    val job = ETLJob(
      name = "Test Job",
      schedule = every(displayName = "test schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(10),
      retries = 0,
      ctx = DummyContext(),
    ) {
      success()
    }
    val isReady = job.schedule.ready(
      refTime = LocalDate.of(2021, 8, 14).atStartOfDay(),
      lastRun = null,
    )
    assert(isReady)
  }

  @Test
  fun toString_happy_path() {
    val job = ETLJob(
      name = "Test Job",
      schedule = every(displayName = "test schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(10),
      retries = 0,
      ctx = DummyContext(),
    ) {
      success()
    }
    assertEquals(expected = expectedToStringResult, actual = job.toString())
  }
}
