package main.kotlin.adapter

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import main.kotlin.domain.LogMessage
import main.kotlin.domain.LogRepository
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction

suspend fun sqlLogger(
  db: Database,
  repository: LogRepository,
  messages: SharedFlow<LogMessage>,
) {
  messages.collect { msg -> transaction(db = db) { repository.add(msg) } }
}
