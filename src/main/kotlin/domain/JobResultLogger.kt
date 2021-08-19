package main.kotlin.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction

class JobResultLogger(
  private val scope: CoroutineScope,
  private val db: Database,
  private val results: SharedFlow<JobResult>,
  private val repository: ResultRepository,
  private val log: LogMessages,
) {
  fun start() {
    scope.launch {
      results.collect { result ->
        try {
          transaction(db = db) { repository.add(result) }
        } catch (e: Exception) {
          log.error(e.stackTraceToString())
        }
      }
    }
  }
}
