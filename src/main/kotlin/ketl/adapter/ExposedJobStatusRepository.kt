package ketl.adapter

import ketl.domain.JobStatus
import ketl.domain.JobStatusName
import ketl.domain.JobStatusRepository
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.update
import java.time.LocalDateTime

class ExposedJobStatusRepository : JobStatusRepository {
  override fun upsert(status: JobStatus) {
    val error = when (status) {
      is JobStatus.Cancelled -> null
      is JobStatus.Failure -> status.errorMessage
      is JobStatus.Initial -> null
      is JobStatus.Running -> null
      is JobStatus.Success -> null
    }

    val ct = JobStatusTable.select { JobStatusTable.jobName eq status.jobName }.count()
    if (ct > 0) {
      JobStatusTable.update({ JobStatusTable.jobName eq status.jobName }) {
        it[JobStatusTable.status] = status.statusName
        it[ts] = LocalDateTime.now()
        it[errorMessage] = error
      }
    } else {

      JobStatusTable.insert {
        it[jobName] = status.jobName
        it[JobStatusTable.status] = JobStatusName.Failed
        it[ts] = LocalDateTime.now()
        it[errorMessage] = error
      }
    }
  }
}
