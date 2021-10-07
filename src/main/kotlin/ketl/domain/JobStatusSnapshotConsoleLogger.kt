package ketl.domain

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
suspend fun jobStatusSnapshotConsoleLogger(
  statuses: JobStatuses,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  launch(dispatcher) {
    statuses.snapshots.collect { snapshot ->
      val running = statusCSV(snapshot = snapshot, status = JobStatusName.Running)
      val failedCount = snapshot.values.count { it.statusName == JobStatusName.Failed }
      val successes = snapshot.values.count { it.statusName == JobStatusName.Successful }
      val success = if (successes > 10) {
        "$successes Jobs"
      } else {
        "[${statusCSV(snapshot = snapshot, status = JobStatusName.Successful)}]"
      }
      val dtFormatter = DateTimeFormatter.ofPattern("M/d @ hh:mm:ss a")
      val ts = LocalDateTime.now().format(dtFormatter)
      val errors = snapshot.values.filterIsInstance<JobStatus.Failure>().sortedBy { it.jobName }
      val errorMessages =
        if (errors.isEmpty()) {
          ""
        } else {
          errors.joinToString("\n  ") { jobStatus ->
            "- [${jobStatus.jobName}] ${jobStatus.ts.format(dtFormatter)}: ${formatErrorMessage(jobStatus.errorMessage)}"
          } + "\n  "
        }
      println(
        "$ts\n  Running: [$running]\n  Success: $success\n  Failed:  $failedCount\n  $errorMessages"
      )
    }
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
