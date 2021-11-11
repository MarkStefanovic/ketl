package ketl.adapter

import ketl.domain.JobStatusName
import ketl.domain.LogLevel
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction

interface Db {
  fun exec(statement: Transaction.() -> Unit)

  fun <R> fetch(statement: Transaction.() -> R): R

  fun createTables()
}

class SQLDb(private val exposedDb: Database) : Db {
  override fun <R> fetch(statement: Transaction.() -> R): R =
    transaction(db = exposedDb) {
      statement()
    }

  override fun exec(statement: Transaction.() -> Unit) {
    transaction(db = exposedDb) {
      statement()
    }
  }

  override fun createTables() {
    transaction(db = exposedDb) {
      SchemaUtils.create(LogTable, JobResultTable, JobStatusTable)
    }
  }
}

object LogTable : Table("ketl_log") {
  val id = integer("id").autoIncrement()
  val name = text("name")
  val level = enumerationByName("level", 20, LogLevel::class)
  val message = text("message")
  val ts = datetime("ts").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(id, name = "pk_ketl_log")

  init {
    index("ix_ketl_log_name", false, name)
    index("ix_ketl_log_level", false, level)
  }
}

object JobDepTable : Table("ketl_job_dep") {
  val jobName = text("job_name")
  val dependency = text("dependency")

  override val primaryKey = PrimaryKey(jobName, dependency, name = "pk_ketl_job_dep")
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

  override val primaryKey = PrimaryKey(id, name = "pk_ketl_result")

  init {
    index("ix_ketl_result_job_name_start", false, jobName, start)
    index("ix_ketl_result_start_failed", false, start, failed)
  }
}

object JobStatusTable : Table("ketl_status") {
  val jobName = text("job_name")
  val status = enumerationByName("status", length = 20, JobStatusName::class)
  val ts = datetime("ts")
  val errorMessage = text("error_message").nullable()

  override val primaryKey = PrimaryKey(jobName, name = "pk_ketl_status")

  init {
    index("ix_ketl_status_status", false, status)
  }
}

object JobSpecTable : Table("ketl_job_spec") {
  val jobName = text("job_name")
  val scheduleName = text("schedule_name")
  val timeoutSeconds = long("timeout_seconds")
  val retries = integer("retries")
  val enabled = bool("enabled")
  val dateAdded = datetime("date_added").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(jobName, name = "pk_ketl_job_spec")
}
