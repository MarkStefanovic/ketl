package ketl.service

import ketl.domain.JobService
import ketl.domain.KETLJob
import ketl.domain.validateJobs
import kotlin.time.ExperimentalTime

@ExperimentalTime
class StaticJobService(private val jobs: Set<KETLJob>) : JobService {
  init {
    val validationResult = validateJobs(jobs)
    if (validationResult.hasErrors) {
      throw Exception(validationResult.errorMessage)
    }
  }

  override fun getActiveJobs() = jobs.shuffled().toSet()

  override fun getJob(jobName: String): KETLJob? = jobs.firstOrNull { it.name == jobName }
}
