package ketl.service

import ketl.domain.JobQueue
import kotlinx.coroutines.flow.collect
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobQueueConsoleLogger(jobQueue: JobQueue) {
  jobQueue.stream.collect { jobs ->
    val jobNamesCSV = jobs.joinToString(", ") { it.name }
    println("Job Queue Status: $jobNamesCSV")
  }
}
