package ketl

import ketl.domain.ETLJob
import ketl.domain.JobContext
import ketl.domain.every
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun createJobs(context: JobContext): List<ETLJob<*>> =
  listOf(
    ETLJob(
      name = "job1",
      schedule = every(displayName = "job1 schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { log ->
      delay(5000)
      log.info("Job1 done sleeping")
      success()
    },
    ETLJob(
      name = "job2",
      schedule = every(displayName = "job2 schedule", frequency = Duration.seconds(7)),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { log ->
      delay(10000)
      log.info("Job2 done sleeping")
      success()
    },
    ETLJob(
      name = "job3",
      schedule = every(displayName = "job3 schedule", frequency = Duration.seconds(11)),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { throw Exception("Whoops!") }
  )

class E2E
