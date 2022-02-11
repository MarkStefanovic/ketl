package ketl.service

import ketl.domain.JobQueue
import ketl.domain.Log
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobQueueLogger(
  jobQueue: JobQueue,
  log: Log,
) {
  jobQueue.stream.collect { jobs ->
    val jobNamesCSV = jobs.joinToString(", ") { it.name }
    log.info(
      message = "Job Queue Status: $jobNamesCSV",
    )
  }
}
