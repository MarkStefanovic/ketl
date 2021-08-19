package main.kotlin.adapter

import main.kotlin.domain.JobStatus
import main.kotlin.domain.JobStatusName
import main.kotlin.domain.JobStatusRepository
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.update
import java.time.LocalDateTime

class ExposedJobStatusRepository : JobStatusRepository {
  override fun upsert(status: JobStatus) {
    val error = when (status) {
      is JobStatus.Failure -> status.errorMessage
      is JobStatus.Initial -> null
      is JobStatus.Running -> null
      is JobStatus.Success -> null
    }

    val ct = JobStatusTable.select { JobStatusTable.jobName eq status.jobName }.count()
    if (ct > 0) {
      JobStatusTable.update({ JobStatusTable.jobName eq status.jobName }) {
        it[this.status] = status.statusName
        it[this.ts] = LocalDateTime.now()
        it[this.errorMessage] = error
      }
    } else {

      JobStatusTable.insert {
        it[this.jobName] = status.jobName
        it[this.status] = JobStatusName.Failed
        it[this.ts] = LocalDateTime.now()
        it[this.errorMessage] = error
      }
    }
  }
}
