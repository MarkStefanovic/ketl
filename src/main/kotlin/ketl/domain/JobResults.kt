package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import java.util.concurrent.ConcurrentHashMap

interface JobResults {
  val stream: SharedFlow<JobResult>

  fun getLatestResultForJob(name: String): JobResult?

  suspend fun add(result: JobResult)
}

object DefaultJobResults : JobResults {
  private val _stream = MutableSharedFlow<JobResult>(
    extraBufferCapacity = 100,
    onBufferOverflow = BufferOverflow.DROP_OLDEST,
  )
  override val stream = _stream.asSharedFlow()

  private val results = ConcurrentHashMap<String, JobResult>(emptyMap())

  override fun getLatestResultForJob(name: String): JobResult? = results[name]

  override suspend fun add(result: JobResult) {
    results[result.jobName] = result
    _stream.emit(result)
  }
}
