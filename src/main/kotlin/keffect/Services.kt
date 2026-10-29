package keffect

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource

// Default services: the clock and the logger every effect can use without declaring
// them in R. Like Effect-TS's `Effect.withClock` / `Logger.replace`, they live in the
// running context (here the coroutine context) and are inherited by forked fibers.

/** Where `Effect.log` writes. Replace it with [withLogger], e.g. to capture logs in tests. */
fun interface Logger {
    fun log(level: String, message: String, fiberId: Int)

    companion object {
        /** `12:00:00 level=INFO fiber=#1 message="..."` on stdout. */
        val console: Logger = Logger { level, message, fiberId ->
            val time = java.time.LocalTime.now().withNano(0)
            println("$time level=$level fiber=#$fiberId message=\"$message\"")
        }
    }
}

private class ClockElement(val timeSource: TimeSource) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ClockElement>
}

private class LoggerElement(val logger: Logger) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<LoggerElement>
}

/** The clock that measures elapsed time (`timed`, `Schedule.upTo`). */
internal suspend fun currentTimeSource(): TimeSource =
    currentCoroutineContext()[ClockElement]?.timeSource ?: TimeSource.Monotonic

/**
 * `Effect.withClock` — measure elapsed time with [timeSource] while this effect runs.
 *
 * Sleeping still follows the coroutine dispatcher, so the two must agree: in tests, run
 * under `runTest` and pass its `testTimeSource`, and both are virtual.
 */
fun <R, E, A> Effect<R, E, A>.withClock(timeSource: TimeSource): Effect<R, E, A> =
    Effect { env -> withContext(ClockElement(timeSource)) { unsafeRun(env) } }

/** `Logger.replace` — send this effect's logs (and its fibers' logs) to [logger]. */
fun <R, E, A> Effect<R, E, A>.withLogger(logger: Logger): Effect<R, E, A> =
    Effect { env -> withContext(LoggerElement(logger)) { unsafeRun(env) } }

fun Effect.Companion.log(message: String, level: String = "INFO"): Effect<Any?, Nothing, Unit> = Effect {
    val logger = currentCoroutineContext()[LoggerElement]?.logger ?: Logger.console
    logger.log(level, message, currentFiberId())
}

fun Effect.Companion.logError(message: String) = log(message, "ERROR")
