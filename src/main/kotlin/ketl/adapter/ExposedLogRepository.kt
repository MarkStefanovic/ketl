package main.kotlin.ketl.adapter

import main.kotlin.ketl.domain.LogMessage
import main.kotlin.ketl.domain.LogRepository
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import java.time.LocalDateTime

class ExposedLogRepository : LogRepository {
  override fun add(message: LogMessage) {
    LogTable.insert {
      it[name] = message.loggerName
      it[level] = message.level
      it[LogTable.message] = message.message
    }
  }

  override fun deleteBefore(ts: LocalDateTime) {
    LogTable.deleteWhere { LogTable.ts lessEq ts }
  }
}
