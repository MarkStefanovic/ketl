package ketl.domain

import ketl.adapter.Db
import ketl.adapter.DbResultRepo
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlin.time.ExperimentalTime

@ExperimentalTime
@DelicateCoroutinesApi
class JobResults(private val db: Db) {
  private val repo = DbResultRepo()

  private val _status = MutableStateFlow<ServiceStatus>(ServiceStatus.Initial)
  val status = _status.asStateFlow()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 1000,
    )

  val stream = _stream.asSharedFlow()

  suspend fun add(result: JobResult) {
    _stream.emit(result)
  }

  suspend fun getLatestResultForJob(jobName: String): JobResult? =
    try {
      db.fetch {
        repo.getLatestResultsForJob(jobName = jobName, n = 1)
      }.getOrThrow().firstOrNull()
    } catch (te: TimeoutCancellationException) {
      println("""
            |JobSpecService.activeJobs(): db.exec timed out
            |  original error message: ${te.message}
          """.trimMargin()
      )
      _status.emit(ServiceStatus.Error(message = "db.exec timed out", originalError = te))
      null
    } catch (e: Exception) {
      println("""
            |JobSpecService.activeJobs(): ${e.message}
            |  ${e.stackTraceToString()}
          """.trimMargin()
      )
      _status.emit(ServiceStatus.Error(message = e.message ?: "No error message provided.", originalError = e))
      null
    }
  }
