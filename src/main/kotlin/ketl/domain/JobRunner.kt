package ketl.domain

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobRunner(
  log: LogMessages,
  queue: SharedFlow<Job<*>>,
  status: JobStatuses,
  results: JobResults,
  maxSimultaneousJobs: Int,
) = coroutineScope {
  //  val dispatcher = Executors.newFixedThreadPool(maxSimultaneousJobs).asCoroutineDispatcher()

  queue.collect { job ->
    if (status.getStatusForJob(job.name) !is JobStatus.Running &&
      status.getRunningJobCount() < maxSimultaneousJobs
    ) {
      status.running(job.name)

      try {
        val result = runJob(log = log, job = job)
        val msg =
          when (result) {
            is JobResult.Cancelled -> "${result.jobName} was cancelled."
            is JobResult.Failure -> "${result.jobName} failed: ${result.errorMessage}"
            is JobResult.Success -> "${result.jobName} finished successfully."
          }
        log.info(msg)

        results.add(result)

        when (result) {
          is JobResult.Cancelled ->
            status.cancel(jobName = job.name)
          is JobResult.Failure ->
            status.failure(jobName = job.name, errorMessage = result.errorMessage)
          is JobResult.Success -> status.success(jobName = job.name)
        }
      } catch (e: Exception) {
        if (e !is CancellationException) {
          log.error(e.stackTraceToString())
          status.failure(
            jobName = job.name,
            errorMessage = e.stackTraceToString(),
          )
        } else {
          log.info("Cancelling job ${job.name}: ${e.message}")
          status.cancel(jobName = job.name)
        }
      }
    }
  }
}

@ExperimentalTime
private suspend fun runJob(log: LogMessages, job: Job<*>): JobResult = supervisorScope {
  log.info("Starting ${job.name}...")

  val start = LocalDateTime.now()

  try {
    withTimeout(job.timeout.inWholeMilliseconds) {
      runWithRetry(
        job = job,
        start = start,
        retries = 0,
      )
    }
  } catch (_: TimeoutCancellationException) {
    JobResult.Failure(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
      errorMessage = "Job timed out after ${job.timeout.inWholeSeconds} seconds.",
    )
  } catch (e: Exception) {
    if (e is CancellationException) {
      log.info("${job.name} cancelled")
      JobResult.Cancelled(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
      )
    } else {
      JobResult.Failure(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.stackTraceToString(),
      )
    }
  }
}

@ExperimentalTime
suspend fun runWithRetry(
  job: Job<*>,
  start: LocalDateTime,
  retries: Int,
): JobResult =
  try {
    job.run()
    JobResult.Success(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
    )
  } catch (e: Exception) {
    if (retries >= job.retries) {
      JobResult.Failure(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.stackTraceToString(),
      )
    } else {
      runWithRetry(
        job = job,
        start = start,
        retries = retries + 1,
      )
    }
  }
