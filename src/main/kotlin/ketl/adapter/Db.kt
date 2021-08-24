package main.kotlin.ketl.adapter

import main.kotlin.ketl.domain.JobStatusName
import main.kotlin.ketl.domain.LogLevel
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.`java-time`.CurrentDateTime
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction

object LogTable : Table("log") {
  val id = integer("id").autoIncrement()
  val name = text("name")
  val level = enumerationByName("level", 20, LogLevel::class)
  val message = text("message")
  val ts = datetime("ts").defaultExpression(CurrentDateTime())

  override val primaryKey = PrimaryKey(id, name = "pk_log_id")
}

object JobResultTable : Table("result") {
  val id = integer("result").autoIncrement("seq_result_id")
  val jobName = text("job_name")
  val start = datetime("start")
  val end = datetime("end")
  val errorFlag = bool("error_flag")
  val errorMessage = text("error_message").nullable()

  override val primaryKey = PrimaryKey(id, name = "pk_result_id")

  init {
    index("ix_result_job_name_ts", false, jobName, start)
    index("ix_result_start_error_flag", false, start, errorFlag)
  }
}

object JobStatusTable : Table("status") {
  val jobName = text("job_name")
  val status = enumerationByName("status", length = 20, JobStatusName::class)
  val ts = datetime("ts")
  val errorMessage = text("error_message").nullable()

  override val primaryKey = PrimaryKey(jobName, name = "pk_status_job_name")

  init {
    index("ix_status_status", false, status)
  }
}

fun Database.exec(sql: String) = transaction(db = this) { exec(sql) }
