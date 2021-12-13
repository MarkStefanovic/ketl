package ketl.service

import ketl.domain.JobService
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import ketl.domain.Log
import ketl.domain.NamedLog
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobStatusCleaner(
  jobService: JobService,
  jobStatuses: JobStatuses,
  timeBetweenScans: Duration,
  log: Log = NamedLog("jobStatusCleaner"),
) {
  while (coroutineContext.isActive) {
    jobStatuses.state.runningJobs().forEach { jobName ->
      val job = jobService.getJob(jobName)

      if (job != null) {
        val jobStatus = jobStatuses.state.currentStatusOf(jobName)

        if (jobStatus != null) {
          val secondsSinceStatus: Long = jobStatus.ts.until(LocalDateTime.now(), ChronoUnit.SECONDS)

          if (secondsSinceStatus > job.timeout.inWholeSeconds) {
            val status = JobStatus.Cancelled(jobName = jobName, ts = LocalDateTime.now())

            jobStatuses.add(status)

            log.debug("Expired job $jobName.")
          }
        }
      }
    }

    delay(timeBetweenScans)
  }
}
