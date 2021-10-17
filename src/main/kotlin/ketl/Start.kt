@file:Suppress("unused")

package ketl

import com.zaxxer.hikari.HikariDataSource
import ketl.adapter.Db
import ketl.adapter.DbJobStatusRepository
import ketl.adapter.DbLogRepository
import ketl.adapter.ExposedResultRepository
import ketl.adapter.SQLDb
import ketl.adapter.exposedLogRepositoryCleaner
import ketl.adapter.exposedResultRepositoryCleaner
import ketl.adapter.sqlLogger
import ketl.domain.ETLJob
import ketl.domain.JobContext
import ketl.domain.JobQueue
import ketl.domain.JobResults
import ketl.domain.JobStatuses
import ketl.domain.LogLevel
import ketl.domain.RootLog
import ketl.domain.SharedLog
import ketl.domain.consoleLogger
import ketl.domain.jobResultLogger
import ketl.domain.jobRunner
import ketl.domain.jobScheduler
import ketl.domain.jobStatusLogger
import ketl.domain.jobStatusSnapshotConsoleLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.io.File
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
private suspend fun startServices(
  db: Db,
  log: SharedLog,
  rootLog: RootLog,
  jobs: List<ETLJob<*>>,
  logStatusToConsole: Boolean,
  maxSimultaneousJobs: Int,
  logCutoff: Duration,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  rootLog.info("Starting ketl services...")

  rootLog.info("Checking if ETL tables exist...")
  db.createTables()

  val logRepository = DbLogRepository()

  rootLog.info("Starting log repository cleaner...")
  launch(dispatcher) {
    exposedLogRepositoryCleaner(
      db = db,
      repository = logRepository,
      log = rootLog,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = logCutoff,
    )
  }

  rootLog.info("Starting SQL logging...")
  launch(dispatcher) {
    sqlLogger(
      db = db,
      repository = logRepository,
      messages = log.stream,
    )
  }

  val statusRepository = DbJobStatusRepository()

  rootLog.info("Starting JobStatuses...")
  val statuses = JobStatuses()

  rootLog.info("Starting console logger...")
  if (logStatusToConsole) {
    launch(dispatcher) {
      jobStatusLogger(
        db = db,
        repository = statusRepository,
        status = statuses.stream,
      )
    }
  }

  rootLog.info("Starting job status console logger...")
  launch(dispatcher) { jobStatusSnapshotConsoleLogger(statuses = statuses) }
  val resultRepository = ExposedResultRepository()

  rootLog.info("Starting result repository cleaner...")
  launch(dispatcher) {
    exposedResultRepositoryCleaner(
      db = db,
      repository = resultRepository,
      log = rootLog,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = logCutoff,
    )
  }

  rootLog.info("Starting JobResults...")
  val results = JobResults(db = db)

  rootLog.info("Starting job result logger...")
  launch(dispatcher) {
    jobResultLogger(
      db = db,
      results = results.stream,
      repository = resultRepository,
    )
  }

  val jobQueue = JobQueue()

  rootLog.info("Starting job scheduler...")
  launch(dispatcher) {
    jobScheduler(
      queue = jobQueue,
      jobs = jobs,
      status = statuses,
      maxSimultaneousJobs = maxSimultaneousJobs,
      scanFrequency = Duration.seconds(10),
      results = results,
    )
  }

  rootLog.info("Starting JobRunner...")
  launch(dispatcher) {
    jobRunner(
      log = log,
      queue = jobQueue.stream,
      results = results,
      status = statuses,
      dispatcher = dispatcher,
    )
  }

  rootLog.info("ketl services launched.")
}

@Suppress("BlockingMethodInNonBlockingContext")
@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
suspend fun <Ctx : JobContext> start(
  createContext: () -> Ctx,
  createJobs: (ctx: Ctx) -> List<ETLJob<*>>,
  createDatasource: () -> HikariDataSource = { sqliteDatasource("./etl.db") },
  maxSimultaneousJobs: Int = 10,
  logJobMessagesToConsole: Boolean = true,
  logStatusChangesToConsole: Boolean = true,
  minLogLevel: LogLevel = LogLevel.Info,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  logCutoff: Duration = Duration.days(7),
) = coroutineScope {
  val log = SharedLog()

  val rootLog = RootLog(log)

  if (logJobMessagesToConsole) {
    launch(dispatcher) {
      println("Starting console logger...")
      consoleLogger(messages = log.stream, minLogLevel = minLogLevel)
    }
  }

  val ctx = createContext()
  try {
    val jobs =
      try {
        createJobs(ctx)
      } catch (e: Throwable) {
        println("An error occurred while creating jobs: ${e.stackTraceToString()}")
        throw e
      }

    val ds = createDatasource()
    val job =
      try {
        rootLog.info("Starting services...")
        launch(dispatcher) {
          startServices(
            db = SQLDb(ds),
            log = log,
            rootLog = rootLog,
            jobs = jobs,
            logStatusToConsole = logStatusChangesToConsole,
            dispatcher = dispatcher,
            maxSimultaneousJobs = maxSimultaneousJobs,
            logCutoff = logCutoff,
          )
        }
      } catch (e: Throwable) {
        ds.close()
        println("Closed connection to ETL database.")
        throw e
      }

    job.invokeOnCompletion {
      it?.printStackTrace()

      try {
        dispatcher.cancelChildren(it as? CancellationException)
        println("Children cancelled.")
      } catch (e: Throwable) {
        println("An exception occurred while closing child processes: ${e.stackTraceToString()}")
      }

      try {
        ds.close()
        println("Closed connection to ETL database.")
      } catch (e: Throwable) {
        println("An error occurred while closing the ETL datasource: ${e.stackTraceToString()}")
      }

      try {
        ctx.close()
        println("Closed job context.")
      } catch (e: Throwable) {
        println(
          "An error occurred while closing the enclosing job context: ${e.stackTraceToString()}"
        )
      } finally {
        exitProcess(1)
      }
    }

    job
  } catch (e: Throwable) {
    try {
      ctx.close()
    } catch (e2: Throwable) {
      println("An error occurred while closing the job context: ${e2.stackTraceToString()}")
    }
    throw e
  }
}

private fun runJar(jarPath: File, jvmArgs: List<String> = emptyList()) {
  val javaHome = System.getProperty("java.home")
  val javaPath = javaHome + File.separator + "bin" + File.separator + "java"
  val cmd = arrayListOf(javaPath, "-jar", jarPath.path, *jvmArgs.toTypedArray())
  ProcessBuilder(cmd)
    .directory(File(jarPath.parent))
    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
    .redirectError(ProcessBuilder.Redirect.INHERIT)
    .start()
    .waitFor()
}

@ExperimentalTime
fun restartJarOnCrash(
  jarPath: File,
  jvmArgs: List<String> = emptyList(),
  timeBetweenRestarts: Duration = Duration.seconds(10),
) {
  while (true) {
    runJar(jarPath = jarPath, jvmArgs = jvmArgs)
    println("Process crashed.  Waiting $timeBetweenRestarts and then restarting...")
    Thread.sleep(timeBetweenRestarts.inWholeMilliseconds)
  }
}
