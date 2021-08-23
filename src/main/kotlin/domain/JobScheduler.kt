package main.kotlin.domain

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobScheduler(
  queue: JobQueue,
  jobs: List<Job<*>>,
  scanFrequency: Duration = Duration.seconds(10),
) = coroutineScope {
  val queueTimes: MutableMap<String, LocalDateTime> = mutableMapOf()

  val lock = ReentrantLock()

  while (true) {
    jobs.forEach { job ->
      lock.withLock {
        if (job.isReady(
            lastRun = queueTimes[job.name],
            refTime = LocalDateTime.now(),
          )
        ) {
          queueTimes[job.name] = LocalDateTime.now()
          launch { queue.add(job) }
        }
      }
    }
    delay(scanFrequency.inWholeMilliseconds)
  }
}
