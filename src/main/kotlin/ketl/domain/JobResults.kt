package ketl.domain

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors

interface JobResults {
  val stream: SharedFlow<JobResult>

  suspend fun getLatestResults(): Set<JobResult>

  suspend fun getLatestResultForJob(name: String): JobResult?

  suspend fun add(result: JobResult)
}

object DefaultJobResults : JobResults {
  private val _stream = MutableSharedFlow<JobResult>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )
  override val stream = _stream.asSharedFlow()

  private val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

  private val results = mutableMapOf<String, JobResult>()

  override suspend fun getLatestResults(): Set<JobResult> =
    withContext(dispatcher) {
      results.values.toSet()
    }

  override suspend fun getLatestResultForJob(name: String): JobResult? =
    withContext(dispatcher) {
      results[name]
    }

  override suspend fun add(result: JobResult) = withContext(dispatcher) {
    results[result.jobName] = result
    _stream.emit(result)
  }
}
