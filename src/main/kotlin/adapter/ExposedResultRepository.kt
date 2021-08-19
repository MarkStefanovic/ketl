package main.kotlin.adapter

import main.kotlin.domain.JobResult
import main.kotlin.domain.ResultRepository
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import java.time.LocalDateTime

class ExposedResultRepository : ResultRepository {
  override fun add(result: JobResult) {
    JobResultTable.insert {
      it[this.jobName] = result.jobName
      it[this.start] = result.start
      it[this.end] = result.end
      it[this.errorFlag] = result is JobResult.Failure
      it[this.errorMessage] = if (result is JobResult.Failure) result.errorMessage else null
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
  if (row[JobResultTable.errorFlag])
    JobResult.Success(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
    )
  else
    JobResult.Failure(
      jobName = row[JobResultTable.jobName],
      start = row[JobResultTable.start],
      end = row[JobResultTable.end],
      errorMessage = row[JobResultTable.errorMessage] ?: "No error message was provided.",
    )
