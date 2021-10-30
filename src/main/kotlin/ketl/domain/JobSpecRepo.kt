package ketl.domain

import kotlin.time.ExperimentalTime

@ExperimentalTime
interface JobSpecRepo {
  fun add(jobSpec: JobSpec)

  fun all(): Set<JobSpec>

  fun get(): JobSpec

  fun update(jobSpec: JobSpec)

  fun upsert(jobSpec: JobSpec)
}
