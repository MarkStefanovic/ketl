package ketl.service

import ketl.domain.JobService
import ketl.domain.KETLJob
import kotlin.time.ExperimentalTime

@ExperimentalTime
class StaticJobService(private val jobs: Set<KETLJob>) : JobService {
  override fun getActiveJobs() = jobs

  override fun getJob(jobName: String): KETLJob? =
    jobs.firstOrNull { it.name == jobName }
}
