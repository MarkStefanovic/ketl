package main.kotlin.ketl.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.ExperimentalTime

@ExperimentalTime
class JobResults(
  private val scope: CoroutineScope,
  private val jobs: List<Job<*>>,
) {
  private val history = mutableMapOf<String, List<JobResult>>()

  private val lock = ReentrantLock()

  private val _stream =
    MutableSharedFlow<JobResult>(
      onBufferOverflow = BufferOverflow.DROP_OLDEST,
      extraBufferCapacity = 10,
    )

  val stream = _stream.asSharedFlow()

  init {
    lock.withLock { jobs.forEach { job -> history[job.name] = emptyList() } }
  }

  suspend fun add(result: JobResult) {
    lock.withLock {
      val priorResults = history[result.jobName]?.take(9) ?: emptyList()
      history[result.jobName] = priorResults + result
      scope.launch { _stream.emit(result) }
    }
  }

  fun getHistoryForJob(jobName: String): List<JobResult> = lock.withLock {
    history.getOrDefault(jobName, emptyList())
  }
}
