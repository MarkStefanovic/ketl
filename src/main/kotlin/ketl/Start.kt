package ketl

import com.zaxxer.hikari.HikariDataSource
import ketl.adapter.Db
import ketl.adapter.DbJobStatusRepository
import ketl.adapter.DbLogRepository
import ketl.adapter.ExposedResultRepository
import ketl.adapter.SingleThreadedDb
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
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
private suspend fun startServices(
  db: Db,
  log: LogMessages,
  jobs: List<Job<*>>,
  logStatusToConsole: Boolean,
  maxSimultaneousJobs: Int,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  log.info("Starting ketl services...")

  log.info("Checking if ETL tables exist...")
  db.createTables().join()

  val logRepository = DbLogRepository()

  log.info("Starting log repository cleaner...")
  launch {
    exposedLogRepositoryCleaner(
      db = db,
      repository = logRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
      dispatcher = dispatcher,
    )
  }

  log.info("Starting SQL logging...")
  launch {
    sqlLogger(
      db = db,
      repository = logRepository,
      messages = log.stream,
      dispatcher = dispatcher,
    )
  }

  val statusRepository = DbJobStatusRepository()

  log.info("Starting JobStatuses...")
  val statuses =
    JobStatuses(
      scope = this,
      jobs = jobs,
      dispatcher = dispatcher,
    )

  log.info("Starting console logger...")
  if (logStatusToConsole) {
    launch {
      jobStatusLogger(
        log = log,
        db = db,
        repository = statusRepository,
        status = statuses.stream,
        dispatcher = dispatcher,
      )
    }
  }

  log.info("Starting job status console logger...")
  launch {
    jobStatusSnapshotConsoleLogger(
      statuses = statuses,
      dispatcher = dispatcher,
    )
  }

  val resultRepository = ExposedResultRepository()

  log.info("Starting result repository cleaner...")
  launch {
    exposedResultRepositoryCleaner(
      db = db,
      repository = resultRepository,
      log = log,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = Duration.days(3),
      dispatcher = dispatcher,
    )
  }

  log.info("Starting JobResults...")
  val results = JobResults(db = db)

  log.info("Starting job result logger...")
  launch {
    jobResultLogger(
      db = db,
      results = results.stream,
      repository = resultRepository,
      log = log,
      dispatcher = dispatcher,
    )
  }

  val jobQueue = JobQueue()

  log.info("Starting job scheduler...")
  launch {
    jobScheduler(
      queue = jobQueue,
      jobs = jobs,
      status = statuses,
      maxSimultaneousJobs = maxSimultaneousJobs,
      scanFrequency = Duration.seconds(10),
      dispatcher = dispatcher,
      results = results,
    )
  }

  log.info("Starting JobRunner...")
  launch {
    jobRunner(
      log = log,
      queue = jobQueue.stream,
      results = results,
      status = statuses,
      dispatcher = dispatcher,
    )
  }

  log.info("ketl services launched.")
}

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
suspend fun start(
  log: LogMessages,
  jobs: List<Job<*>>,
  maxSimultaneousJobs: Int,
  ds: HikariDataSource = sqliteDatasource(),
  logJobMessagesToConsole: Boolean = true,
  logStatusChangesToConsole: Boolean = true,
  minLogLevel: LogLevel = LogLevel.Info,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  if (logJobMessagesToConsole) {
    println("Starting console logger...")
    launch(dispatcher) {
      consoleLogger(
        messages = log.stream,
        minLogLevel = minLogLevel,
        dispatcher = dispatcher,
      )
    }
  }

  log.info("Starting services...")
  launch(dispatcher) {
    startServices(
      db = SingleThreadedDb(ds),
      log = log,
      jobs = jobs,
      logStatusToConsole = logStatusChangesToConsole,
      dispatcher = dispatcher,
      maxSimultaneousJobs = maxSimultaneousJobs,
    )
  }
}
