package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.time.LocalDateTime
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobRunner(
  log: LogMessages,
  queue: SharedFlow<Job<*>>,
  status: JobStatuses,
  results: JobResults,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
    queue.collect { job ->
      launch {
        runJob(
          log = log,
          job = job,
          results = results,
          status = status,
        )
      }
    }
  }
}

@DelicateCoroutinesApi
@ExperimentalTime
private suspend fun runJob(
  log: LogMessages,
  job: Job<*>,
  results: JobResults,
  status: JobStatuses,
) {
  log.info("Starting ${job.name}...")

  val start = LocalDateTime.now()

  status.running(job.name)

  try {
    withTimeout(job.timeout.inWholeMilliseconds) {
      val result =
        runWithRetry(
          job = job,
          start = start,
          retries = 0,
        )
      val msg =
        when (result) {
          is JobResult.Cancelled -> "${result.jobName} was cancelled."
          is JobResult.Failure -> "${result.jobName} failed: ${result.errorMessage}"
          is JobResult.Success -> "${result.jobName} finished successfully."
        }
      log.info(msg)

      results.add(result)

      when (result) {
        is JobResult.Cancelled -> status.cancel(jobName = job.name)
        is JobResult.Failure ->
          status.failure(jobName = job.name, errorMessage = result.errorMessage)
        is JobResult.Success -> status.success(jobName = job.name)
      }
    }
  } catch (_: TimeoutCancellationException) {
    val result =
      JobResult.Failure(
        jobName = job.name,
        start = start,
        end = LocalDateTime.now(),
        errorMessage = "Job timed out after ${job.timeout.inWholeSeconds} seconds.",
      )
    results.add(result)
  } catch (e: Exception) {
    if (e is CancellationException) {
      log.info("${job.name} cancelled")
      val result =
        JobResult.Cancelled(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
        )
      results.add(result)
      status.cancel(jobName = job.name)
    } else {
      val result =
        JobResult.Failure(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
          errorMessage = e.stackTraceToString(),
        )
      results.add(result)
      status.failure(jobName = job.name, errorMessage = result.errorMessage)
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
