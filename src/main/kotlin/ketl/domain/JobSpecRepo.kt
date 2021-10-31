package ketl.domain

import kotlin.time.ExperimentalTime

@ExperimentalTime
interface JobSpecRepo {
  fun add(jobSpec: JobSpec)

  fun delete(jobName: String)

  fun getActiveJobs(): Set<JobSpec>

  fun get(jobName: String): JobSpec?

  fun update(jobSpec: JobSpec)

  fun upsert(jobSpec: JobSpec) {
    val current = get(jobSpec.jobName)
    if (current == null) {
      add(jobSpec)
    } else {
      if (current != jobSpec) {
        update(jobSpec)
      }
    }
  }
}
