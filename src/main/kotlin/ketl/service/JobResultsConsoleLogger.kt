package ketl.service

import ketl.domain.JobResults
import kotlinx.coroutines.flow.collect

suspend fun jobResultsConsoleLogger(jobResults: JobResults) {
  jobResults.stream.collect { jobResult ->
    println("Job result received: $jobResult")
  }
}
