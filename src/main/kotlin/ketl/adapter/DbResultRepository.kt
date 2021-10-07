package ketl.adapter

import ketl.domain.JobResult
import ketl.domain.ResultRepository
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import java.time.LocalDateTime

class ExposedResultRepository : ResultRepository {
  override fun add(result: JobResult) {
    JobResultTable.insert {
      it[jobName] = result.jobName
      it[start] = result.start
      it[end] = result.end
      it[cancelled] = result is JobResult.Cancelled
      it[failed] = result is JobResult.Failure
      it[skipped] = result is JobResult.Skipped
      it[errorMessage] = if (result is JobResult.Failure) {
        result.errorMessage
      } else {
        null
      }
      it[skippedReason] = if (result is JobResult.Skipped) {
        result.reason
      } else {
        null
      }
    }
  }

  override fun deleteBefore(ts: LocalDateTime) {
    JobResultTable.deleteWhere { JobResultTable.start lessEq ts }
  }

  override fun getLatestResultsForJob(jobName: String, n: Int): List<JobResult> =
    JobResultTable //
      .select { JobResultTable.jobName eq jobName }
      .orderBy(JobResultTable.start, SortOrder.DESC)
      .limit(n)
      .map(::jobResultRowToDomain)
}

private fun jobResultRowToDomain(row: ResultRow) =
  if (row[JobResultTable.failed]) {
    JobResult.Failure(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
      errorMessage = row[JobResultTable.errorMessage] ?: "No error message was provided.",
    )
  } else if (row[JobResultTable.cancelled]) {
    JobResult.Cancelled(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
    )
  } else if (row[JobResultTable.skipped]) {
    JobResult.Skipped(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
      reason = row[JobResultTable.skippedReason] ?: "No skipped reason was provided.",
    )
  } else {
    JobResult.Success(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
    )
  }
