@file:Suppress("unused")

package ketl

import ketl.adapter.pg.PgLogRepo
import ketl.adapter.sqlite.SQLiteLogRepo
import ketl.domain.DbDialect
import ketl.domain.DefaultJobQueue
import ketl.domain.DefaultJobResults
import ketl.domain.DefaultJobStatuses
import ketl.domain.JobService
import ketl.domain.LogLevel
import ketl.domain.LogMessage
import ketl.domain.LogMessages
import ketl.domain.NamedLog
import ketl.domain.defaultLogFormat
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.service.consoleLogger
import ketl.service.dbJobResultsLogger
import ketl.service.dbJobStatusLogger
import ketl.service.dbLogger
import ketl.service.heartbeat
import ketl.service.jobStatusCleaner
import ketl.service.jobStatusLogger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import java.time.LocalDateTime
import javax.sql.DataSource
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime

@ExperimentalCoroutinesApi
@DelicateCoroutinesApi
@ExperimentalTime
suspend fun start(
  jobService: JobService,
  logDs: DataSource?,
  logDialect: DbDialect?,
  logSchema: String?,
  maxSimultaneousJobs: Int = 10,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  timeBetweenScans: Duration = 10.seconds,
  logJobMessagesToConsole: Boolean = false,
  logJobStatusChanges: Boolean = false,
  minLogLevel: LogLevel = LogLevel.Info,
  minTimeBetweenRestarts: Duration = 5.minutes,
  checkForHeartbeatEvery: Duration = 5.minutes,
  considerDeadIfNoHeartbeatFor: Duration = 15.minutes,
  showSQL: Boolean = false,
) = coroutineScope {
  if (logDs != null) {
    require(logDialect != null) {
      "If a logDs is provided, then logDialect is required."
    }
  }

  while (isActive) {
    val scope = CoroutineScope(Job() + Dispatchers.Default)

    val logMessages = LogMessages()

    val log = NamedLog(
      name = "ketl",
      logMessages = logMessages,
      minLogLevel = minLogLevel,
    )

    val jobStatuses = DefaultJobStatuses()
    val jobResults = DefaultJobResults()
    val jobQueue = DefaultJobQueue()

    val services = scope.launch {
      if (logJobMessagesToConsole) {
        consoleLogger(
          minLogLevel = minLogLevel,
          logMessages = logMessages.stream,
        )
        log.info("Launched consoleLogger.")

        yield()
      }

      if ((logDs != null) && (logDialect != null)) {
        dbLogger(
          dbDialect = logDialect,
          ds = logDs,
          schema = logSchema,
          minLogLevel = minLogLevel,
          logMessages = logMessages,
          showSQL = showSQL,
        )
        log.info("Launched dbLogger.")

        log.info("Launching dbJobResultsLogger...")
        dbJobResultsLogger(
          dbDialect = logDialect,
          ds = logDs,
          schema = logSchema,
          logMessages = logMessages,
          jobResults = jobResults,
          minLogLevel = minLogLevel,
        )

        yield()
      }

      log.info("Launching jobScheduler...")
      jobScheduler(
        log = NamedLog(
          name = "jobScheduler",
          logMessages = logMessages,
          minLogLevel = minLogLevel,
        ),
        jobService = jobService,
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )

      yield()

      log.info("Launching jobStatusCleaner...")
      jobStatusCleaner(
        log = NamedLog(
          name = "jobStatusCleaner",
          logMessages = logMessages,
          minLogLevel = minLogLevel,
        ),
        jobService = jobService,
        jobStatuses = jobStatuses,
        timeBetweenScans = timeBetweenScans,
      )

      yield()

      if (logJobStatusChanges) {
        log.info("Launching jobStatusLogger...")
        jobStatusLogger(
          jobStatuses = jobStatuses,
          log = NamedLog(
            name = "jobStatusLogger",
            logMessages = logMessages,
            minLogLevel = minLogLevel,
          ),
        )

        yield()
      }

      if (logDs != null) {
        log.info("Launching dbJobStatusLogger...")
        dbJobStatusLogger(
          ds = logDs,
          dbDialect = logDialect!!,
          schema = logSchema,
          jobStatuses = jobStatuses,
          logMessages = logMessages,
          minLogLevel = minLogLevel,
        )

        yield()
      }

      log.info("Launching jobRunner...")
      jobRunner(
        queue = jobQueue,
        results = jobResults,
        statuses = jobStatuses,
        logMessages = logMessages,
        dispatcher = dispatcher,
        maxSimultaneousJobs = maxSimultaneousJobs,
        minLogLevel = minLogLevel,
      )
    }

    val heartbeat = scope.launch {
      log.info("Launching heartbeat...")
      heartbeat(
        log = NamedLog(
          name = "heartbeat",
          logMessages = logMessages,
          minLogLevel = minLogLevel,
        ),
        jobResults = jobResults,
        maxTimeToWait = considerDeadIfNoHeartbeatFor,
        timeBetweenChecks = checkForHeartbeatEvery,
      )
    }

    heartbeat.invokeOnCompletion {
      val message = LogMessage(
        loggerName = "ketl",
        level = LogLevel.Error,
        message = "Heartbeat stopped.  Restarting in ${minTimeBetweenRestarts.inWholeSeconds} seconds...",
        ts = LocalDateTime.now(),
      )

      println(defaultLogFormat(message))

      if ((logDs != null) && (logDialect != null)) {
        val logRepo = when (logDialect) {
          DbDialect.PostgreSQL -> PgLogRepo(ds = logDs, schema = logSchema ?: "public")
          DbDialect.SQLite -> SQLiteLogRepo(ds = logDs)
        }

        logRepo.createTable()

        logRepo.add(message)
      }
    }

    heartbeat.join()
    services.join()
    scope.cancel()

    delay(minTimeBetweenRestarts)
  }
}
