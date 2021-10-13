package ketl.adapter

import com.zaxxer.hikari.HikariDataSource
import ketl.domain.JobStatusName
import ketl.domain.LogLevel
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.concurrent.Executors

abstract class Db {
  abstract suspend fun exec(statement: Transaction.() -> Unit): Job

  abstract fun <R> fetch(statement: Transaction.() -> R): R

  abstract suspend fun <R> fetchAsync(statement: Transaction.() -> R): Deferred<R>

  abstract suspend fun createTables(): Job
}

class SingleThreadedDb(private val ds: HikariDataSource) : Db() {
  private val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

  private val db: Database
    get() = Database.connect(ds)

  override fun <R> fetch(statement: Transaction.() -> R): R =
    transaction(db = db) {
      statement()
    }

  override suspend fun <R> fetchAsync(statement: Transaction.() -> R): Deferred<R> = coroutineScope {
    async(dispatcher) {
      transaction(db = db) {
        statement()
      }
    }
  }

  override suspend fun exec(statement: Transaction.() -> Unit) = coroutineScope {
    launch(dispatcher) {
      transaction(db = db) {
        statement()
      }
    }
  }

  override suspend fun createTables() = coroutineScope {
    launch(dispatcher) {
      transaction(db = db) {
        SchemaUtils.create(LogTable, JobResultTable, JobStatusTable)
      }
    }
  }

//  suspend fun execSQL(sql: String) = coroutineScope {
//    launch(dispatcher) {
//      transaction(db = db) { exec(sql) }
//    }
//  }
}

object LogTable : Table("ketl_log") {
  val id = integer("id").autoIncrement()
  val name = text("name")
  val level = enumerationByName("level", 20, LogLevel::class)
  val message = text("message")
  val ts = datetime("ts").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(id, name = "pk_log_id")
}

object JobResultTable : Table("ketl_result") {
  val id = integer("result").autoIncrement("seq_result_id")
  val jobName = text("job_name")
  val start = datetime("start")
  val end = datetime("end")
  val failed = bool("failure_flag")
  val cancelled = bool("cancelled_flag")
  val skipped = bool("skipped_flag")
  val errorMessage = text("error_message").nullable()
  val skippedReason = text("skipped_reason").nullable()

  override val primaryKey = PrimaryKey(id, name = "pk_result_id")

  init {
    index("ix_result_job_name_ts", false, jobName, start)
    index("ix_result_start_error_flag", false, start, failed)
  }
}

object JobStatusTable : Table("ketl_status") {
  val jobName = text("job_name")
  val status = enumerationByName("status", length = 20, JobStatusName::class)
  val ts = datetime("ts")
  val errorMessage = text("error_message").nullable()

  override val primaryKey = PrimaryKey(jobName, name = "pk_status_job_name")

  init {
    index("ix_status_status", false, status)
  }
}
