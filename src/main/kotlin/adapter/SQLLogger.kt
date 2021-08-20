package main.kotlin.adapter

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import main.kotlin.domain.LogMessage
import main.kotlin.domain.LogRepository
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction

class SQLLogger(
  scope: CoroutineScope,
  private val db: Database,
  private val repository: LogRepository,
  private val messages: SharedFlow<LogMessage>,
) {
  init {
    scope.launch {
      messages.collect { msg ->
        transaction(db = db) {
          repository.add(msg)
        }
      }
    }
  }
}
