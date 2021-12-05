package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobRunner(
  queue: JobQueue,
  results: JobResults,
  statuses: JobStatuses,
  log: Log,
  dispatcher: CoroutineDispatcher,
  maxSimultaneousJobs: Int,
  timeBetweenScans: Duration,
) = coroutineScope {
  while (coroutineContext.isActive) {
    while (statuses.runningJobCount < maxSimultaneousJobs) {
      val job = queue.pop()
      if (job != null) {
        launch(dispatcher) {
          runJob(
            results = results,
            statuses = statuses,
            job = job,
            log = log,
          )
        }
      }
      delay(10)
    }
    delay(timeBetweenScans)
  }
}

@DelicateCoroutinesApi
@ExperimentalTime
private suspend fun runJob(
  results: JobResults,
  statuses: JobStatuses,
  log: Log,
  job: KETLJob,
) = withTimeout(timeout = job.timeout) {
  val start = LocalDateTime.now()

  try {
    log.debug(name = job.name, "Starting ${job.name}...")

    statuses.add(JobStatus.Running(jobName = job.name, ts = start))

    val result = runWithRetry(
      job = job,
      log = log,
      start = start,
      retries = 0,
    )
    results.add(result)

    when (result) {
      is JobResult.Cancelled -> {
        statuses.add(
          JobStatus.Cancelled(
            jobName = job.name,
            ts = LocalDateTime.now(),
          )
        )
        log.info(name = job.name, "${result.jobName} was cancelled.")
      }
      is JobResult.Failed -> {
        statuses.add(
          JobStatus.Failed(
            jobName = job.name,
            ts = LocalDateTime.now(),
            errorMessage = result.errorMessage,
          )
        )
        log.error(name = job.name, "${result.jobName} failed: ${result.errorMessage}")
      }
      is JobResult.Success -> {
        statuses.add(
          JobStatus.Success(
            jobName = job.name,
            ts = LocalDateTime.now(),
          )
        )
        log.debug(name = job.name, "${result.jobName} finished successfully.")
      }
      is JobResult.Skipped -> {
        statuses.add(
          JobStatus.Skipped(
            jobName = job.name,
            ts = LocalDateTime.now(),
            reason = result.reason,
          )
        )
        log.info(name = job.name, "${result.jobName} was skipped.")
      }
    }

    log.debug(name = job.name, "Finished ${job.name}")
  } catch (ce: CancellationException) {
    log.error(name = job.name, "${job.name} cancelled: ${ce.message}")
  } catch (e: Exception) {
    statuses.add(
      JobStatus.Failed(
        jobName = job.name,
        ts = LocalDateTime.now(),
        errorMessage = e.message ?: "No error message was provided.",
      )
    )
    results.add(
      JobResult.Failed(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.message ?: "No error message was provided.",
      )
    )
    log.error(name = job.name, "An unexpected error occurred while running ${job.name}: ${e.message}")
  }
}

@ExperimentalTime
suspend fun runWithRetry(
  job: KETLJob,
  log: Log,
  retries: Int,
  start: LocalDateTime,
): JobResult =
  try {
    when (val status: Status = job.run(log)) {
      is Status.Failure -> {
        if (retries >= job.maxRetries) {
          JobResult.Failed(
            jobName = job.name,
            start = start,
            end = LocalDateTime.now(),
            errorMessage = status.errorMessage,
          )
        } else {
          runWithRetry(
            job = job,
            log = log,
            start = start,
            retries = retries + 1,
          )
        }
      }
      is Status.Skipped ->
        JobResult.Skipped(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
          reason = status.reason,
        )
      Status.Success ->
        JobResult.Success(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
        )
    }
  } catch (e: Throwable) {
    if (retries + 1 >= job.maxRetries) {
      JobResult.Failed(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = e.stackTraceToString(),
      )
    } else {
      log.info(name = job.name, "${job.name} threw an exception, running retry ${retries + 1} of ${job.maxRetries}...")
      runWithRetry(
        job = job,
        log = log,
        start = start,
        retries = retries + 1,
      )
    }
  }
