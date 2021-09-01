package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobScheduler(
  queue: JobQueue,
  jobs: List<Job<*>>,
  scanFrequency: Duration = Duration.seconds(10),
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  val queueTimes: ConcurrentHashMap<String, LocalDateTime> = ConcurrentHashMap()

  launch(dispatcher) {
    while (true) {
      jobs.forEach { job ->
        if (job.isReady(
            lastRun = queueTimes[job.name],
            refTime = LocalDateTime.now(),
          )
        ) {
          queueTimes[job.name] = LocalDateTime.now()
          launch { queue.add(job) }
        }
      }
      delay(scanFrequency.inWholeMilliseconds)
    }
  }
}
