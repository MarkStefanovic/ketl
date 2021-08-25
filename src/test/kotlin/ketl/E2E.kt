package ketl

import ketl.domain.Job
import ketl.domain.JobContext
import ketl.domain.Schedule
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun createJobs(context: JobContext): List<Job<*>> =
  listOf(
    Job(
      name = "job1",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(10)),
        Schedule(frequency = Duration.seconds(25)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) {
      delay(5000)
      log.info("Job1 done sleeping")
    },
    Job(
      name = "job2",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(7)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) {
      delay(10000)
      log.info("Job2 done sleeping")
    },
    Job(
      name = "job3",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(11)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { throw Exception("Whoops!") }
  )

class E2E
