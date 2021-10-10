package ketl.domain

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.delay
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
) {
  val queueTimes: ConcurrentHashMap<String, LocalDateTime> = ConcurrentHashMap()

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
            queue.add(job)
          }
        }
      }
    }
    delay(scanFrequency.inWholeMilliseconds)
  }
}

@DelicateCoroutinesApi
@ExperimentalTime
fun dependenciesHaveRun(
  jobName: String,
  dependencies: Set<String>,
  results: JobResults,
): Boolean {
  val latestResult = results.getLatestResultForJob(jobName)
  return if (dependencies.isEmpty()) {
    true
  } else {
    dependencies.any { dep ->
      val latestDepResult = results.getLatestResultForJob(dep)
      if (latestResult == null) {
        true
      } else {
        if (latestDepResult == null) {
          false
        } else {
          if (latestDepResult is JobResult.Success) {
            latestDepResult.end > latestResult.end
          } else {
            false
          }
        }
      }
    }
  }
}
