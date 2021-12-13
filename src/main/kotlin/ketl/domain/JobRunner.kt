package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.SharedFlow
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
  logMessages: SharedFlow<LogMessage>,
  dispatcher: CoroutineDispatcher,
  maxSimultaneousJobs: Int,
  timeBetweenScans: Duration,
) = coroutineScope {
  val log = NamedLog("jobRunner")

  while (coroutineContext.isActive) {
    val runningJobs = statuses.state.runningJobs()
    log.debug("Running jobs: $runningJobs")

    while (statuses.state.runningJobs().count() < maxSimultaneousJobs) {
      val job = queue.pop()

      if (job == null) {
        log.debug("No job was found in the queue.")
      } else {
        log.debug("Starting ${job.name}...")
        val jobLog = NamedLog(name = job.name, stream = logMessages)

        launch(dispatcher) {
          runJob(
            results = results,
            statuses = statuses,
            job = job,
            log = jobLog,
          )
        }
      }
      delay(1000)
    }

    log.debug("Waiting ${timeBetweenScans.inWholeSeconds} seconds to scan again.")

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
    log.debug("Starting ${job.name}...")

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
        log.info("${result.jobName} was cancelled.")
      }
      is JobResult.Failed -> {
        statuses.add(
          JobStatus.Failed(
            jobName = job.name,
            ts = LocalDateTime.now(),
            errorMessage = result.errorMessage,
          )
        )
        log.error("${result.jobName} failed: ${result.errorMessage}")
      }
      is JobResult.Successful -> {
        statuses.add(
          JobStatus.Success(
            jobName = job.name,
            ts = LocalDateTime.now(),
          )
        )
        log.debug("${result.jobName} finished successfully.")
      }
      is JobResult.Skipped -> {
        statuses.add(
          JobStatus.Skipped(
            jobName = job.name,
            ts = LocalDateTime.now(),
            reason = result.reason,
          )
        )
        log.info("${result.jobName} was skipped.")
      }
    }

    log.debug("Finished ${job.name}")
  } catch (ce: CancellationException) {
    log.error("${job.name} cancelled: ${ce.message}")
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
    log.error("An unexpected error occurred while running ${job.name}: ${e.message}\n${e.stackTraceToString()}")
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
        JobResult.Successful(
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
      log.info("${job.name} threw an exception, running retry ${retries + 1} of ${job.maxRetries}...")
      runWithRetry(
        job = job,
        log = log,
        start = start,
        retries = retries + 1,
      )
    }
  }
