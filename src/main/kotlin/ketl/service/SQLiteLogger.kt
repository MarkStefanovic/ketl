package ketl.service

import ketl.adapter.SQLiteLogRepo
import ketl.domain.DefaultLog
import ketl.domain.Log
import ketl.domain.LogLevel
import ketl.domain.gte
import kotlinx.coroutines.flow.collect
import javax.sql.DataSource

suspend fun sqliteLogger(
  ds: DataSource,
  log: Log = DefaultLog,
  minLogLevel: LogLevel = LogLevel.Info,
) {
  val repo = SQLiteLogRepo(ds = ds)

  ds.connection.use { connection ->
    repo.createLogTable(con = connection)
  }

  log.stream.collect { logMessage ->
    if (logMessage.level gte minLogLevel) {
      repo.add(logMessage)
    }
  }
}
