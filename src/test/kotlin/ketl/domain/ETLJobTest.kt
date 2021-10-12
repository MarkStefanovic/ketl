package ketl.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDate
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

class DummyContext(log: LogMessages) : JobContext(log) {
  override fun close() {}
}

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ETLJobTest {
  @Test
  fun isReady_happy_path() {
    val log = LogMessages("test")

    val job = ETLJob(
      name = "Test Job",
      schedule = listOf(
        Schedule(Duration.seconds(10)),
      ),
      timeout = Duration.seconds(10),
      retries = 0,
      ctx = DummyContext(log),
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
