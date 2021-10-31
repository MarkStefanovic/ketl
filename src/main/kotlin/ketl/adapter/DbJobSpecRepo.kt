package ketl.adapter

import ketl.domain.JobSpec
import ketl.domain.JobSpecRepo
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.update
import java.time.LocalDateTime
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class DbJobSpecRepo : JobSpecRepo {
  override fun add(jobSpec: JobSpec) {
    JobSpecTable.insert {
      it[jobName] = jobSpec.jobName
      it[scheduleName] = jobSpec.scheduleName
      it[timeoutSeconds] = jobSpec.timeout.inWholeSeconds
      it[retries] = jobSpec.retries
      it[enabled] = jobSpec.enabled
      it[dateAdded] = LocalDateTime.now()
    }
  }

  override fun delete(jobName: String) {
    JobDepTable.deleteWhere { JobDepTable.jobName eq jobName }
    JobSpecTable.deleteWhere { JobSpecTable.jobName eq jobName }
  }

  override fun getActiveJobs(): Set<JobSpec> {
    val dependencies =
      JobDepTable
        .selectAll()
        .groupBy({ it[JobDepTable.jobName] }) { it[JobDepTable.dependency] }
    return JobSpecTable
      .select { JobSpecTable.enabled eq true }
      .map { row ->
        JobSpec(
          jobName = row[JobSpecTable.jobName],
          scheduleName = row[JobSpecTable.scheduleName],
          timeout = Duration.seconds(row[JobSpecTable.timeoutSeconds]),
          retries = row[JobSpecTable.retries],
          enabled = row[JobSpecTable.enabled],
          dependencies = dependencies[row[JobSpecTable.jobName]]?.toSet() ?: setOf(),
        )
      }
      .toSet()
  }

  override fun get(jobName: String): JobSpec? {
    val deps =
      JobDepTable
        .select { JobDepTable.jobName eq jobName }
        .map { it[JobDepTable.dependency] }
        .toSet()
    return JobSpecTable
      .select { JobSpecTable.jobName eq jobName }
      .limit(1)
      .map { row ->
        JobSpec(
          jobName = row[JobSpecTable.jobName],
          scheduleName = row[JobSpecTable.scheduleName],
          timeout = Duration.seconds(row[JobSpecTable.timeoutSeconds]),
          retries = row[JobSpecTable.retries],
          enabled = row[JobSpecTable.enabled],
          dependencies = deps,
        )
      }
      .firstOrNull()
  }

  override fun update(jobSpec: JobSpec) {
    val currentDeps =
      JobDepTable
        .select { JobDepTable.jobName eq jobSpec.jobName }
        .map { it[JobDepTable.dependency] }
        .toSet()

    if (jobSpec.dependencies != currentDeps) {
      JobDepTable.deleteWhere { JobDepTable.jobName eq jobSpec.jobName }
      JobDepTable.batchInsert(jobSpec.dependencies) { dep ->
        this[JobDepTable.jobName] = jobSpec.jobName
        this[JobDepTable.dependency] = dep
      }
    }

    JobSpecTable.update({ JobSpecTable.jobName eq jobSpec.jobName }) {
      it[jobName] = jobSpec.jobName
      it[scheduleName] = jobSpec.scheduleName
      it[timeoutSeconds] = jobSpec.timeout.inWholeSeconds
      it[retries] = jobSpec.retries
      it[enabled] = jobSpec.enabled
    }
  }
}
