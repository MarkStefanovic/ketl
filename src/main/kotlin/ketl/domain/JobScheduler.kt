package ketl.domain

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import java.time.LocalDateTime
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobScheduler(
  jobService: JobService,
  queue: JobQueue,
  results: JobResults,
  timeBetweenScans: Duration,
) {
  while (coroutineContext.isActive) {
    jobService.getActiveJobs().forEach { job ->
      val ready = job.schedule.ready(
        lastRun = results.getLatestResultForJob(job.name)?.end,
        refTime = LocalDateTime.now(),
      )
      val depsRan = dependenciesHaveRun(
        jobName = job.name,
        dependencies = job.dependencies,
        results = results,
      )
      if (ready && depsRan) {
        queue.add(job)
      } else {
        queue.drop(job.name)
      }
    }
    delay(timeBetweenScans)
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
