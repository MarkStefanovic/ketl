package ketl.domain

import kotlin.time.ExperimentalTime

@ExperimentalTime
interface JobService {
  fun getActiveJobs(): Set<KETLJob>
}
