package main.kotlin.domain

import kotlinx.coroutines.flow.collect
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.ExperimentalTime

@ExperimentalTime
suspend fun jobStatusSnapshotConsoleLogger(statuses: JobStatuses) {
  statuses.snapshots.collect { snapshot ->
    val running = statusCSV(snapshot = snapshot, status = JobStatusName.Running)
    val failed = statusCSV(snapshot = snapshot, status = JobStatusName.Failed)
    val success = statusCSV(snapshot = snapshot, status = JobStatusName.Successful)
    val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a"))
    val errors = snapshot.values.filterIsInstance<JobStatus.Failure>()
    val errorMessages =
      if (errors.isEmpty()) {
        ""
      } else {
        errors.joinToString("\n") { jobStatus ->
          "- [${jobStatus.jobName}]: ${formatErrorMessage(jobStatus.errorMessage)}"
        } + "\n"
      }
    println(
      """$ts
          Running: [$running]
          Success: [$success]
          Failed:  [$failed]
          $errorMessages
      """.trimIndent()
    )
  }
}

private fun statusCSV(snapshot: Snapshot, status: JobStatusName): String =
  snapshot
    .values
    .asSequence()
    .filter { it.statusName == status }
    .map { it.jobName }
    .sorted()
    .joinToString(", ")

private fun formatErrorMessage(errorMessage: String): String {
  val cleanString = errorMessage.replace("\n", "; ").replace("\t", "")
  return if (cleanString.length > 100) {
    cleanString.substring(0..96) + "..."
  } else {
    cleanString
  }
}
