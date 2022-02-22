package ketl.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
fun CoroutineScope.jobScheduler(
  jobService: JobService,
  queue: JobQueue,
  results: JobResults,
  statuses: JobStatuses,
  timeBetweenScans: Duration,
  log: Log,
) = launch {
  while (isActive) {
    val activeJobs = jobService.getActiveJobs()

    val validationResult = validateJobs(activeJobs)

    if (validationResult.hasErrors) {
      log.error(validationResult.errorMessage)
    }

    val validJobNames = validationResult.validJobNames

    val validJobs = activeJobs.filter { it.name in validJobNames }

    validJobs.forEach { job ->
      if (job.name !in statuses.state.runningJobs()) {
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
    }
    delay(timeBetweenScans)
  }
}

@DelicateCoroutinesApi
@ExperimentalTime
suspend fun dependenciesHaveRun(
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
          if (latestDepResult is JobResult.Successful) {
            latestDepResult.end > latestResult.end
          } else {
            false
          }
        }
      }
    }
  }
}
