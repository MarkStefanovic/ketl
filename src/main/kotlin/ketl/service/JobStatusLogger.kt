package ketl.service

import ketl.domain.DefaultJobStatuses
import ketl.domain.JobStatus
import ketl.domain.JobStatuses
import ketl.domain.Log
import ketl.domain.NamedLog
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobStatusLogger(
  jobStatuses: JobStatuses = DefaultJobStatuses,
  log: Log = NamedLog("jobStatusLogger"),
) {
  val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

  val latestStatuses = mutableMapOf<String, JobStatus>()

  jobStatuses.stream.statuses.collect { status: JobStatus ->
    val statuses = withContext(dispatcher) {
      latestStatuses[status.jobName] = status
      latestStatuses
    }

    val namesStatusMap =
      statuses.map { (key, value) ->
        key to when (value) {
          is JobStatus.Cancelled -> "Cancelled"
          is JobStatus.Failed -> "Failed"
          is JobStatus.Initial -> "Initial"
          is JobStatus.Running -> "Running"
          is JobStatus.Skipped -> "Skipped"
          is JobStatus.Success -> "Success"
        }
      }
        .toMap()
    val running = statusCSV(statuses = namesStatusMap, status = "Running")
    val success = statusCSV(statuses = namesStatusMap, status = "Success")
    val skipped = statusCSV(statuses = namesStatusMap, status = "Skipped")
    val failed = statusCSV(statuses = namesStatusMap, status = "Failed")
    val dtFormatter = DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a")
    val ts = LocalDateTime.now().format(dtFormatter)
    val errors = statuses.values.filterIsInstance<JobStatus.Failed>().sortedBy { it.jobName }
    val errorMessages =
      if (errors.isEmpty()) {
        ""
      } else {
        errors.joinToString("\n  ") { jobStatus ->
          "- [${jobStatus.jobName}] ${jobStatus.ts.format(dtFormatter)}: ${formatErrorMessage(jobStatus.errorMessage)}"
        } + "\n  "
      }
    log.info(
      """
        |$ts  
        |  Running: $running
        |  Success: $success
        |  Skipped: $skipped
        |  Failed:  $failed
        |  $errorMessages
      """.trimMargin(),
    )
  }
}

private fun statusCSV(statuses: Map<String, String>, status: String): String {
  val selected =
    statuses
      .filter { (_, statusName) ->
        statusName == status
      }
      .keys

  return if (selected.count() < 10) {
    selected
      .sorted()
      .joinToString(", ")
  } else {
    "${selected.count()} jobs"
  }
}

private fun formatErrorMessage(errorMessage: String): String {
  val cleanString = errorMessage.replace("\n", "; ").replace("\t", "")
  return if (cleanString.length > 100) {
    cleanString.substring(0..96) + "..."
  } else {
    cleanString
  }
}
