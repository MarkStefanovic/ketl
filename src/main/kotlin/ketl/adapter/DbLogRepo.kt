package ketl.adapter

import ketl.domain.LogMessage
import ketl.domain.LogRepo
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import java.time.LocalDateTime

class DbLogRepo : LogRepo {
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
