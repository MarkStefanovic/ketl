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
        try {
          runJob(
            log = log,
            job = job,
            results = results,
            status = status,
          )
        } catch (e: Exception) {
          if (e is CancellationException) {
            log.info("jobRunner cancelled.")
            throw e
          } else {
            log.error(e.stackTraceToString())
            throw e
          }
        }
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
) = coroutineScope {
  log.debug("Starting ${job.name}...")

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
      when (result) {
        is JobResult.Cancelled -> log.info("${result.jobName} was cancelled.")
        is JobResult.Failure -> log.error("${result.jobName} failed: ${result.errorMessage}")
        is JobResult.Success -> log.debug("${result.jobName} finished successfully.")
        is JobResult.Skipped -> log.info("${result.jobName} was skipped.")
      }

      results.add(result)

      when (result) {
        is JobResult.Cancelled -> status.cancel(jobName = job.name)
        is JobResult.Failure ->
          status.failure(jobName = job.name, errorMessage = result.errorMessage)
        is JobResult.Success -> status.success(jobName = job.name)
        is JobResult.Skipped -> status.skipped(jobName = job.name, reason = result.reason)
      }
    }
  } catch (_: TimeoutCancellationException) {
    val result = JobResult.Failure(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
      errorMessage = "Job timed out after ${job.timeout.inWholeSeconds} seconds.",
    )
    results.add(result)
  } catch (e: Exception) {
    when (e) {
      is CancellationException -> {
        log.info("${job.name} cancelled")
        val result = JobResult.Cancelled(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
        )
        results.add(result)
        status.cancel(jobName = job.name)
        throw e
      }
      is OutOfMemoryError -> {
        println("Ran out of memory while running ${job.name}.")
        throw e
      }
      else -> {
        val result = JobResult.Failure(
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
}

@ExperimentalTime
suspend fun runWithRetry(
  job: Job<*>,
  start: LocalDateTime,
  retries: Int,
): JobResult =
  when (val status = job.run()) {
    is Status.Failure -> {
      if (retries >= job.retries) {
        JobResult.Failure(
          jobName = job.name,
          start = start,
          end = LocalDateTime.now(),
          errorMessage = status.errorMessage,
        )
      } else {
        runWithRetry(
          job = job,
          start = start,
          retries = retries + 1,
        )
      }
    }
    is Status.Skipped -> JobResult.Skipped(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
      reason = status.reason,
    )
    Status.Success -> JobResult.Success(
      jobName = job.name,
      start = start,
      end = LocalDateTime.now(),
    )
  }
