package ketl

import ketl.adapter.Db
import ketl.adapter.DbJobSpecRepo
import ketl.domain.JobSpec
import ketl.domain.ServiceStatus
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
interface AbstractJobSpecService {
  suspend fun start()

  suspend fun pop(n: Int): Set<JobSpec>
}

@ExperimentalTime
class JobSpecService(
  private val db: Db,
  val refreshFrequency: Duration = Duration.minutes(5),
) : AbstractJobSpecService {

  val jobSpecs = ConcurrentLinkedQueue<JobSpec>()

  private val _status = MutableStateFlow<ServiceStatus>(ServiceStatus.Initial)
  val status = _status.asStateFlow()

  private val repo by lazy {
    db.createJobSpecServiceTables()

    DbJobSpecRepo()
  }

  override suspend fun start() = coroutineScope {
    while (coroutineContext.isActive) {
      try {
        val jobSpecs = db.fetch {
          repo.getActiveJobs()
        }.getOrThrow()

        _status.emit(ServiceStatus.Ok)

        jobSpecs.plus(jobSpecs)
      } catch (te: TimeoutCancellationException) {
        _status.emit(ServiceStatus.Error(message = "db.exec timed out", originalError = te))

        println(
          """
          |JobSpecService.activeJobs(): db.exec timed out
          |  original error message: ${te.message}
        """.trimMargin()
        )
      } catch (e: Exception) {
        _status.emit(ServiceStatus.Error(message = e.message ?: "No error message provided", originalError = e))

        println(
          """
          |JobSpecService.activeJobs(): ${e.message}
          |  ${e.stackTraceToString()}
        """.trimMargin()
        )
      }
    }
    delay(refreshFrequency.inWholeMilliseconds)
  }

  override suspend fun pop(n: Int): Set<JobSpec> =
    jobSpecs.minus(jobSpecs.take(n)).toSet()
}
