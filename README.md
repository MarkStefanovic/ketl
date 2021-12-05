```kotlin 
import ketl.domain.KETLJob
import ketl.domain.Log
import ketl.domain.LogLevel
import ketl.domain.Schedule
import ketl.domain.Status
import ketl.domain.every
import ketl.service.StaticJobService
import ketl.service.consoleLogger
import ketl.service.jobQueueLogger
import ketl.service.jobResultsLogger
import ketl.service.jobStatusLogger
import ketl.start
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class DummyJob(
  override val name: String,
  override val dependencies: Set<String>,
  override val maxRetries: Int,
  override val schedule: Schedule,
  override val timeout: Duration,
  val sleep: Duration,
) : KETLJob {
  override suspend fun run(log: Log): Status {
    print(name)
    log.info(name = name, message = "Running $name...")
    delay(sleep)
    val roll = Random.nextInt(0, 100)
    if (roll > 75) {
      throw Exception("Rolled a $roll.")
    }
    log.info(name = name, message = "Finished running $name.")
    return Status.Success
  }
}

@ExperimentalTime
val jobs: Set<KETLJob> =
  setOf(
    DummyJob(
      name = "job1",
      schedule = every(displayName = "job1 schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(60),
      maxRetries = 0,
      dependencies = setOf(),
      sleep = Duration.seconds(5),
    ),
    DummyJob(
      name = "job2",
      schedule = every(displayName = "job2 schedule", frequency = Duration.seconds(7)),
      dependencies = setOf("job1"),
      timeout = Duration.seconds(60),
      maxRetries = 0,
      sleep = Duration.seconds(10),
    ),
    DummyJob(
      name = "job3",
      schedule = every(displayName = "job3 schedule", frequency = Duration.seconds(11)),
      dependencies = setOf("job2"),
      timeout = Duration.seconds(60),
      maxRetries = 0,
      sleep = Duration.seconds(3),
    ),
    DummyJob(
      name = "job4",
      schedule = every(displayName = "job4 schedule", frequency = Duration.seconds(11)),
      timeout = Duration.seconds(60),
      maxRetries = 3,
      dependencies = setOf(),
      sleep = Duration.seconds(10),
    ),
    DummyJob(
      name = "job5",
      schedule = every(displayName = "job5 schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(60),
      maxRetries = 0,
      dependencies = setOf(),
      sleep = Duration.seconds(5),
    ),
    DummyJob(
      name = "job6",
      schedule = every(displayName = "job6 schedule", frequency = Duration.seconds(10)),
      timeout = Duration.seconds(60),
      maxRetries = 0,
      dependencies = setOf(),
      sleep = Duration.seconds(5),
    ),
  )

@DelicateCoroutinesApi
@ExperimentalTime
@InternalCoroutinesApi
fun main(args: Array<String>) = runBlocking {
  launch {
    jobStatusLogger()
  }

  launch {
    consoleLogger(minLogLevel = LogLevel.Debug)
  }

//  launch {
//    jobQueueLogger()
//  }
//
//  launch {
//    jobResultsLogger()
//  }

  start(
    jobService = StaticJobService(jobs),
    maxSimultaneousJobs = 4,
  ).cancelAndJoin()
}
```