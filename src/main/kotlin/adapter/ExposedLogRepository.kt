package main.kotlin.adapter

import main.kotlin.domain.LogMessage
import main.kotlin.domain.LogRepository
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import java.time.LocalDateTime

class ExposedLogRepository : LogRepository {
  override fun add(message: LogMessage) {
    LogTable.insert {
      it[this.name] = message.loggerName
      it[this.level] = message.level
      it[this.message] = message.message
    }
  }

  override fun deleteBefore(ts: LocalDateTime) {
    LogTable.deleteWhere { LogTable.ts lessEq ts }
  }
}
