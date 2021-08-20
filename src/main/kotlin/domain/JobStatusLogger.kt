package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.time.ExperimentalTime

@ExperimentalTime
@InternalCoroutinesApi
class JobStatusLogger(
  scope: CoroutineScope,
  private val log: LogMessages,
  private val db: Database,
  private val repository: JobStatusRepository,
  private val status: SharedFlow<JobStatus>,
) {
  init {
    scope.launch {
      status.collect { jobStatus ->
        try {
          transaction(db = db) {
            repository.upsert(jobStatus)
          }
        } catch (e: Exception) {
          log.error(e.stackTraceToString())
        }
      }
    }
  }
}
