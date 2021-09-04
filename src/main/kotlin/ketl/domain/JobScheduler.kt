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
  jobs: List<Job<*>>,
  status: JobStatuses,
  queue: JobQueue,
  results: JobResults,
  maxSimultaneousJobs: Int,
  scanFrequency: Duration = Duration.seconds(10),
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  val queueTimes: ConcurrentHashMap<String, LocalDateTime> = ConcurrentHashMap()

  launch(dispatcher) {
    while (true) {
      var jobsToAdd = maxSimultaneousJobs - status.runningJobCount()
      if (jobsToAdd > 0) {
        jobs.sortedBy { queueTimes[it.name] ?: LocalDateTime.MIN }.forEach { job ->
          if (jobsToAdd > 0) {
            val ready = job.isReady(
              lastRun = queueTimes[job.name],
              refTime = LocalDateTime.now(),
            )
            val depsRan = dependenciesHaveRun(
              dependencies = job.dependencies,
              results = results.latestResults,
              lastRun = results.latestResults[job.name]?.end
            )
            if (ready && depsRan) {
              jobsToAdd--
              queueTimes[job.name] = LocalDateTime.now()
              launch {
                queue.add(job)
              }
            }
          }
        }
      }
      delay(scanFrequency.inWholeMilliseconds)
    }
  }
}

fun dependenciesHaveRun(
  dependencies: Set<String>,
  results: ConcurrentHashMap<String, JobResult>,
  lastRun: LocalDateTime?,
): Boolean =
  if (dependencies.isEmpty()) {
    true
  } else {
    dependencies.any { jobName ->
      val latestResult = results[jobName]
      if (latestResult == null) {
        false
      } else {
        latestResult.end > lastRun
      }
    }
  }
