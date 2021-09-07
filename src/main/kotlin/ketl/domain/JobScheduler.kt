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
              jobName = job.name,
              dependencies = job.dependencies,
              results = results,
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

@DelicateCoroutinesApi
@ExperimentalTime
suspend fun dependenciesHaveRun(
  jobName: String,
  dependencies: Set<String>,
  results: JobResults,
): Boolean {
  val lastRun = results.getLatestResultForJob(jobName)?.end
  return if (dependencies.isEmpty()) {
    true
  } else {
    dependencies.any { dep ->
      val latestResult = results.getLatestResultForJob(dep)
      if (latestResult == null) {
        false
      } else {
        if (lastRun == null) {
          true
        } else {
          latestResult.end > lastRun
        }
      }
    }
  }
}
