package ketl

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import ketl.adapter.ExposedJobStatusRepository
import ketl.adapter.ExposedLogRepository
import ketl.adapter.ExposedResultRepository
import ketl.adapter.JobResultTable
import ketl.adapter.JobStatusTable
import ketl.adapter.LogTable
import ketl.adapter.exposedLogRepositoryCleaner
import ketl.adapter.exposedResultRepositoryCleaner
import ketl.adapter.sqlLogger
import ketl.domain.Job
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobStatuses
import ketl.domain.LogLevel
import ketl.domain.LogMessages
import ketl.domain.consoleLogger
import ketl.domain.jobResultLogger
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.domain.jobStatusLogger
import ketl.domain.jobStatusSnapshotConsoleLogger
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.transactions.transactionManager
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@InternalCoroutinesApi
@ExperimentalTime
private suspend fun startServices(
  db: Database,
  log: LogMessages,
  jobs: List<Job<*>>,
  maxSimultaneousJobs: Int,
  logStatusToConsole: Boolean,
) = coroutineScope {
  log.info("Starting ketl services...")

  log.info("Checking if ETL tables exist...")
  transaction(db = db) { SchemaUtils.create(LogTable, JobResultTable, JobStatusTable) }

  val logRepository = ExposedLogRepository()
  launch {
    exposedLogRepositoryCleaner(
      db = db,
      repository = logRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  }

  log.info("Starting SQL logging...")
  launch {
    sqlLogger(
      db = db,
      repository = logRepository,
      messages = log.stream,
    )
  }

  val statusRepository = ExposedJobStatusRepository()
  val statuses = JobStatuses(scope = this, jobs = jobs)

  if (logStatusToConsole)
    launch {
      jobStatusLogger(
        log = log,
        db = db,
        repository = statusRepository,
        status = statuses.stream,
      )
    }

  launch { jobStatusSnapshotConsoleLogger(statuses = statuses) }

  val results = JobResults(scope = this, jobs = jobs)
  val resultRepository = ExposedResultRepository()
  launch {
    exposedResultRepositoryCleaner(
      db = db,
      repository = resultRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  }

  launch {
    jobResultLogger(
      db = db,
      results = results.stream,
      repository = resultRepository,
      log = log,
    )
  }

  val jobQueue = JobQueue()

  launch {
    jobScheduler(
      queue = jobQueue,
      jobs = jobs,
      scanFrequency = Duration.seconds(10),
    )
  }

  log.info("Starting JobRunner with $maxSimultaneousJobs max simultaneous jobs...")

  launch {
    jobRunner(
      log = log,
      queue = jobQueue.stream,
      results = results,
      status = statuses,
      maxSimultaneousJobs = maxSimultaneousJobs,
    )
  }

  log.info("ketl services launched.")
}

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
suspend fun start(
  jobs: List<Job<*>>,
  logToConsole: Boolean = true,
  minLogLevel: LogLevel = LogLevel.Info,
) = coroutineScope {
  val log = LogMessages("ketl")

  if (logToConsole)
    launch {
      consoleLogger(
        messages = log.stream,
        minLogLevel = minLogLevel,
      )
    }

  val hikariConfig =
    HikariConfig().apply {
      jdbcUrl = "jdbc:sqlite:./etl.db"
      driverClassName = "org.sqlite.JDBC"

      maximumPoolSize = 5
      addDataSourceProperty("cachePrepStmts", "true")
      addDataSourceProperty("prepStmtCacheSize", "250")
      addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    }

  HikariDataSource(hikariConfig).use { ds ->
    val db = Database.connect(ds)
    db.transactionManager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE

    startServices(
      db = db,
      log = log,
      jobs = jobs,
      maxSimultaneousJobs = 2,
      logStatusToConsole = true,
    )
  }

  //  awaitCancellation()
}
