package ketl

import ketl.domain.ETLJob
import ketl.domain.JobContext
import ketl.domain.Schedule
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun createJobs(context: JobContext): List<ETLJob<*>> =
  listOf(
    ETLJob(
      name = "job1",
      schedule =
      listOf(
        Schedule(displayName = "job1 schedule A", frequency = Duration.seconds(10)),
        Schedule(displayName = "job1 schedule B", frequency = Duration.seconds(25)),
      ),
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
      schedule =
      listOf(
        Schedule(displayName = "job2 schedule", frequency = Duration.seconds(7)),
      ),
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
      schedule =
      listOf(
        Schedule(displayName = "job3 schedule", frequency = Duration.seconds(11)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { throw Exception("Whoops!") }
  )

class E2E
