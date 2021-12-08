package ketl.service

import ketl.domain.DefaultJobResults
import ketl.domain.JobResults
import ketl.domain.Log
import ketl.domain.NamedLog
import kotlinx.coroutines.flow.collect

suspend fun jobResultsLogger(
  jobResults: JobResults = DefaultJobResults,
  log: Log = NamedLog("jobResultsLogger"),
) {
  jobResults.stream.collect { jobResult ->
    log.info("Job result received: $jobResult")
  }
}
