@file:Suppress("unused")

package ketl

import ketl.adapter.Db
import ketl.adapter.DbJobStatusRepo
import ketl.adapter.DbLogRepo
import ketl.adapter.DbResultRepo
import ketl.adapter.SQLDb
import ketl.adapter.dbLogRepoCleaner
import ketl.adapter.dbLogger
import ketl.adapter.dbResultRepoCleaner
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
import org.jetbrains.exposed.sql.Database
import java.io.File
import kotlin.system.exitProcess
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

@DelicateCoroutinesApi
@InternalCoroutinesApi
@ExperimentalTime
private suspend fun <Ctx : JobContext> startServices(
  db: Db,
  log: SharedLog,
  rootLog: RootLog,
  context: Ctx,
  createJobs: (Ctx) -> List<ETLJob<Ctx>>,
  logStatusToConsole: Boolean,
  maxSimultaneousJobs: Int,
  logCutoff: Duration,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
) = coroutineScope {
  rootLog.info("Starting ketl services...")

  rootLog.info("Checking if ETL tables exist...")
  db.createBaseTables()

  val logRepository = DbLogRepo()

  rootLog.info("Starting log repository cleaner...")
  launch(dispatcher) {
    dbLogRepoCleaner(
      db = db,
      repository = logRepository,
      log = rootLog,
      timeBetweenCleanup = Duration.hours(1),
      durationToKeep = logCutoff,
    )
  }

  rootLog.info("Starting SQL logging...")
  launch(dispatcher) {
    dbLogger(
      db = db,
      repository = logRepository,
      messages = log.stream,
    )
  }

  val statusRepository = DbJobStatusRepo()

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
  val resultRepository = DbResultRepo()

  rootLog.info("Starting result repository cleaner...")
  launch(dispatcher) {
    dbResultRepoCleaner(
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
      context = context,
      queue = jobQueue,
      createJobs = createJobs,
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
  createJobs: (Ctx) -> List<ETLJob<Ctx>>,
  etlDbConnector: (Ctx) -> Database = { Database.connect(url = "jdbc:sqlite:./etl.db", driver = "org.sqlite.JDBC") },
  maxSimultaneousJobs: Int = 10,
  logJobMessagesToConsole: Boolean = true,
  logStatusChangesToConsole: Boolean = true,
  minLogLevel: LogLevel = LogLevel.Info,
  dispatcher: CoroutineDispatcher = Dispatchers.Default,
  sqlTimeout: Duration = Duration.minutes(15),
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
    val etlDb = etlDbConnector(ctx)

    rootLog.info("Starting services...")

    val job = launch(dispatcher) {
      startServices(
        db = SQLDb(exposedDb = etlDb, timeout = sqlTimeout),
        log = log,
        rootLog = rootLog,
        context = ctx,
        createJobs = createJobs,
        logStatusToConsole = logStatusChangesToConsole,
        dispatcher = dispatcher,
        maxSimultaneousJobs = maxSimultaneousJobs,
        logCutoff = logCutoff,
      )
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
