package ketl.service

import ketl.adapter.SQLiteLogRepo
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.gte
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.collect
import javax.sql.DataSource

suspend fun sqliteLogger(
  ds: DataSource,
  logMessages: SharedFlow<LogMessage> = LogMessages.stream,
  minLogLevel: LogLevel = LogLevel.Info,
) {
  val repo = SQLiteLogRepo(ds = ds)

  ds.connection.use { connection ->
    repo.createLogTable(con = connection)
  }

  logMessages.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      repo.add(logMessage)
    }
  }
}
