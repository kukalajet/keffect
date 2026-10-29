package keffect

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeSource

/**
 * A value carried by the running coroutine context and inherited by forked fibers: how the
 * library threads the clock, the logger, the current scope, the layer memo map and the
 * current fiber through a run without putting them in R.
 */
internal class ContextSlot<T : Any> {
    private class Element<T>(val value: T, key: CoroutineContext.Key<*>) : AbstractCoroutineContextElement(key)

    private val key = object : CoroutineContext.Key<Element<T>> {}

    /** The value installed by the nearest enclosing [with], if any. */
    suspend fun current(): T? = currentCoroutineContext()[key]?.value

    /** Run [block] with [value] installed. */
    suspend fun <R> with(value: T, block: suspend () -> R): R = withContext(Element(value, key)) { block() }
}

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

private val clocks = ContextSlot<TimeSource>()
private val loggers = ContextSlot<Logger>()

/** The clock that measures elapsed time (`timed`, `Schedule.upTo`). */
internal suspend fun currentTimeSource(): TimeSource =
    clocks.current() ?: TimeSource.Monotonic

/**
 * `Effect.withClock` — measure elapsed time with [timeSource] while this effect runs.
 *
 * Sleeping still follows the coroutine dispatcher, so the two must agree: in tests, run
 * under `runTest` and pass its `testTimeSource`, and both are virtual.
 */
fun <R, E, A> Effect<R, E, A>.withClock(timeSource: TimeSource): Effect<R, E, A> =
    Effect { env -> clocks.with(timeSource) { unsafeRun(env) } }

/** `Logger.replace` — send this effect's logs (and its fibers' logs) to [logger]. */
fun <R, E, A> Effect<R, E, A>.withLogger(logger: Logger): Effect<R, E, A> =
    Effect { env -> loggers.with(logger) { unsafeRun(env) } }

fun Effect.Companion.log(message: String, level: String = "INFO"): Effect<Any?, Nothing, Unit> = Effect {
    (loggers.current() ?: Logger.console).log(level, message, currentFiberId())
}

fun Effect.Companion.logError(message: String) = log(message, "ERROR")
