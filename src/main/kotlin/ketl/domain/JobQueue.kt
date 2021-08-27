package ketl.domain

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobQueue {
  private val _stream =
    MutableSharedFlow<Job<*>>(
      replay = 0,
      extraBufferCapacity = 1000,
      onBufferOverflow = BufferOverflow.DROP_LATEST,
    )

  val stream = _stream.asSharedFlow()

  suspend fun add(job: Job<*>) {
    _stream.emit(job)
  }
}
