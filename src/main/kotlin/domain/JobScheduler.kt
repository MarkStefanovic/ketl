package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobScheduler(
  scope: CoroutineScope,
  private val queue: JobQueue,
  private val jobs: List<Job<*>>,
  private val scanFrequency: Duration = Duration.seconds(10),
) {
  private val queueTimes: MutableMap<String, LocalDateTime> = mutableMapOf()

  private val lock = ReentrantLock()

  init {
    scope.launch {
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
  }
}
