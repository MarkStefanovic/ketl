package main.kotlin

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import main.kotlin.adapter.ExposedJobStatusRepository
import main.kotlin.adapter.ExposedLogRepository
import main.kotlin.adapter.ExposedLogRepositoryCleaner
import main.kotlin.adapter.ExposedResultRepository
import main.kotlin.adapter.ExposedResultRepositoryCleaner
import main.kotlin.adapter.JobResultTable
import main.kotlin.adapter.JobStatusTable
import main.kotlin.adapter.LogTable
import main.kotlin.adapter.SQLLogger
import main.kotlin.domain.BaseContext
import main.kotlin.domain.Job
import main.kotlin.domain.JobContext
import main.kotlin.domain.JobQueue
import main.kotlin.domain.JobResultLogger
import main.kotlin.domain.JobResults
import main.kotlin.domain.JobRunner
import main.kotlin.domain.JobScheduler
import main.kotlin.domain.JobStatusLogger
import main.kotlin.domain.JobStatusSnapshotConsoleLogger
import main.kotlin.domain.JobStatuses
import main.kotlin.domain.LogMessages
import main.kotlin.domain.Schedule
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.transactions.transactionManager
import java.sql.Connection
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun createJobs(context: JobContext): List<Job<*>> =
  listOf(
    Job(
      name = "job1",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(10)),
        Schedule(frequency = Duration.seconds(25)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) {
      delay(5000)
      log.info("Job1 done sleeping")
    },
    Job(
      name = "job2",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(7)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) {
      delay(10000)
      log.info("Job2 done sleeping")
    },
    Job(
      name = "job3",
      schedule =
      listOf(
        Schedule(frequency = Duration.seconds(11)),
      ),
      timeout = Duration.seconds(60),
      retries = 0,
      ctx = context,
    ) { throw Exception("Whoops!") }
  )

@InternalCoroutinesApi
@ExperimentalTime
suspend fun startServices(
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
  val logRepositoryCleaner =
    ExposedLogRepositoryCleaner(
      scope = this,
      db = db,
      repository = logRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  logRepositoryCleaner.start()

  log.info("Starting SQL logging...")
  val sqlLogger =
    SQLLogger(
      scope = this,
      db = db,
      repository = logRepository,
      messages = log.stream,
    )
  sqlLogger.start()

  val statusRepository = ExposedJobStatusRepository()
  val statuses = JobStatuses(scope = this, jobs = jobs)

  if (logStatusToConsole)
    JobStatusLogger(
      scope = this,
      log = log,
      db = db,
      repository = statusRepository,
      status = statuses.stream,
    )
      .start()

  JobStatusSnapshotConsoleLogger(
    scope = this,
    statuses = statuses,
  )
    .start()

  val results = JobResults(scope = this, jobs = jobs)
  val resultRepository = ExposedResultRepository()
  val resultRepositoryCleaner =
    ExposedResultRepositoryCleaner(
      scope = this,
      db = db,
      repository = resultRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
    )
  resultRepositoryCleaner.start()

  val resultLogger =
    JobResultLogger(
      scope = this,
      db = db,
      results = results.stream,
      repository = resultRepository,
      log = log,
    )
  resultLogger.start()

  val jobQueue = JobQueue()

  val scheduler =
    JobScheduler(
      scope = this,
      queue = jobQueue,
      jobs = jobs,
      scanFrequency = Duration.seconds(10),
    )
  scheduler.start()

  log.info("Starting JobRunner with $maxSimultaneousJobs max simultaneous jobs...")

  val jobRunner =
    JobRunner(
      scope = this,
      log = log,
      queue = jobQueue.stream,
      results = results,
      status = statuses,
      maxSimultaneousJobs = maxSimultaneousJobs,
    )
  jobRunner.start()

  log.info("ketl services launched.")
}

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
fun main(): Unit = runBlocking {
  val log = LogMessages("ketl")

  val context = BaseContext(log)

  val jobs = createJobs(context)

  val hikariConfig =
    HikariConfig().apply {
      jdbcUrl = "jdbc:sqlite:./etl.db"
      driverClassName = "org.sqlite.JDBC"

      maximumPoolSize = 10
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

  awaitCancellation()
}
