package ketl.service

import ketl.domain.DefaultJobResults
import ketl.domain.DefaultLog
import ketl.domain.JobResults
import ketl.domain.Log
import kotlinx.coroutines.flow.collect

suspend fun jobResultsLogger(
  jobResults: JobResults = DefaultJobResults,
  log: Log = DefaultLog,
) {
  jobResults.stream.collect { jobResult ->
    log.info(
      name = "jobResultsLogger",
      message = "Job result received: $jobResult",
    )
  }
}
