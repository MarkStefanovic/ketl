package ketl

import ketl.adapter.Db
import ketl.adapter.DbJobSpecRepo
import ketl.adapter.JobDepTable
import ketl.adapter.JobSpecTable
import ketl.domain.JobSpec
import org.jetbrains.exposed.sql.SchemaUtils
import kotlin.time.ExperimentalTime

@ExperimentalTime
interface AbstractJobSpecService {
  fun activeJobs(): Set<JobSpec>

  fun addJob(jobSpec: JobSpec)

  fun delete(jobName: String)

  fun get(jobName: String): JobSpec?

  fun updateJob(jobSpec: JobSpec)

  fun upsertJob(jobSpec: JobSpec)
}

@ExperimentalTime
class JobSpecService(private val db: Db) : AbstractJobSpecService {
  private val repo by lazy {
    db.exec {
      SchemaUtils.create(JobDepTable, JobSpecTable)
    }

    DbJobSpecRepo()
  }

  override fun activeJobs(): Set<JobSpec> = db.fetch { repo.getActiveJobs() }

  override fun addJob(jobSpec: JobSpec) = db.exec { repo.add(jobSpec) }

  override fun delete(jobName: String) = db.exec { repo.delete(jobName) }

  override fun get(jobName: String) = db.fetch { repo.get(jobName) }

  override fun updateJob(jobSpec: JobSpec) = db.exec { repo.update(jobSpec) }

  override fun upsertJob(jobSpec: JobSpec) = db.exec { repo.upsert(jobSpec) }
}
