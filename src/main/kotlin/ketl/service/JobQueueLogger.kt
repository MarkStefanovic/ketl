package ketl.service

import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultLog
import ketl.domain.JobQueue
import ketl.domain.Log
import kotlinx.coroutines.flow.collect
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobQueueLogger(
  jobQueue: JobQueue = DefaultJobQueue,
  log: Log = DefaultLog,
) {
  jobQueue.stream.collect { jobs ->
    val jobNamesCSV = jobs.joinToString(", ") { it.name }
    log.info(
      name = "jobQueueLogger",
      message = "Job Queue Status: $jobNamesCSV",
    )
  }
}
