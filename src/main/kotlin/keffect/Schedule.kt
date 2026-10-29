package keffect

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.TimeMark

/**
 * `Schedule` — decides whether to go again and how long to wait.
 *
 * [next] receives the 1-based number of the upcoming recurrence, the latest input
 * (the error for `retry`, the value for `repeat`) and the time elapsed since the start.
 * It returns the delay before the next attempt, or `null` to stop.
 */
fun interface Schedule<in I> {
    fun next(attempt: Int, input: I, elapsed: Duration): Duration?

    companion object {
        /** Recur forever with no delay. */
        val forever: Schedule<Any?> = Schedule { _, _, _ -> ZERO }

        /** Recur at most [n] times. */
        fun recurs(n: Int): Schedule<Any?> = Schedule { attempt, _, _ -> if (attempt <= n) ZERO else null }

        val once: Schedule<Any?> = recurs(1)

        /** Wait [interval] between recurrences, forever. */
        fun spaced(interval: Duration): Schedule<Any?> = Schedule { _, _, _ -> interval }

        /** base, base*factor, base*factor², ... */
        fun exponential(base: Duration, factor: Double = 2.0): Schedule<Any?> =
            Schedule { attempt, _, _ -> base * Math.pow(factor, (attempt - 1).toDouble()) }

        /** base, base, 2*base, 3*base, 5*base, ... */
        fun fibonacci(base: Duration): Schedule<Any?> = Schedule { attempt, _, _ ->
            var a = 1L; var b = 1L
            repeat(attempt - 1) { val t = a + b; a = b; b = t }
            base * a.toDouble()
        }

        /** Keep going only while the input matches. */
        fun <I> recurWhile(predicate: (I) -> Boolean): Schedule<I> =
            Schedule { _, input, _ -> if (predicate(input)) ZERO else null }
    }
}

/** `Schedule.intersect` — both must want to continue; waits for the longer delay. */
infix fun <I> Schedule<I>.and(that: Schedule<I>): Schedule<I> = Schedule { n, i, e ->
    val a = next(n, i, e) ?: return@Schedule null
    val b = that.next(n, i, e) ?: return@Schedule null
    maxOf(a, b)
}

/** `Schedule.union` — continue if either wants to; waits for the shorter delay. */
infix fun <I> Schedule<I>.or(that: Schedule<I>): Schedule<I> = Schedule { n, i, e ->
    val a = next(n, i, e)
    val b = that.next(n, i, e)
    when {
        a == null -> b
        b == null -> a
        else -> minOf(a, b)
    }
}

/** Only continue while [predicate] holds for the input (e.g. only retry transient errors). */
fun <I> Schedule<I>.whileInput(predicate: (I) -> Boolean): Schedule<I> =
    Schedule { n, i, e -> if (predicate(i)) next(n, i, e) else null }

/** Stop once [max] has elapsed since the first attempt. */
fun <I> Schedule<I>.upTo(max: Duration): Schedule<I> =
    Schedule { n, i, e -> if (e >= max) null else next(n, i, e) }

/** Never wait longer than [max] between recurrences. */
fun <I> Schedule<I>.capDelay(max: Duration): Schedule<I> =
    Schedule { n, i, e -> next(n, i, e)?.let { minOf(it, max) } }

/** Randomize each delay by ±[factor] (default ±20%), drawing from [random]. */
fun <I> Schedule<I>.jittered(factor: Double = 0.2, random: Random = Random): Schedule<I> =
    Schedule { n, i, e -> next(n, i, e)?.let { it * (1 + random.nextDouble(-factor, factor)) } }

/** Call [f] with each decision, e.g. for logging. */
fun <I> Schedule<I>.onDecision(f: (attempt: Int, input: I, delay: Duration?) -> Unit): Schedule<I> =
    Schedule { n, i, e -> next(n, i, e).also { f(n, i, it) } }

// ---- Using schedules -----------------------------------------------------------

/**
 * One run of a schedule: tracks the attempt number and the time since the start, and
 * does the waiting. [next] waits as the schedule says and returns `true`, or returns
 * `false` once the schedule is done.
 */
private class Recurrence<in I>(private val schedule: Schedule<I>, private val start: TimeMark) {
    private var attempt = 1

    suspend fun next(input: I): Boolean {
        val wait = schedule.next(attempt++, input, start.elapsedNow()) ?: return false
        // Inside extensions on Effect, a bare `delay(...)` resolves to Effect.delay, hence the qualified name.
        if (wait.isPositive()) kotlinx.coroutines.delay(wait) else kotlinx.coroutines.yield() // stay fair on zero delays
        return true
    }
}

private suspend fun <I> Schedule<I>.start(): Recurrence<I> = Recurrence(this, currentTimeSource().markNow())

/** `Effect.retry(schedule)` — retry on *typed* failures. Defects and interruption are not retried. */
fun <R, E, A> Effect<R, E, A>.retry(schedule: Schedule<E>): Effect<R, E, A> = Effect { env ->
    val recurrence = schedule.start()
    while (true) {
        val cause = when (val exit = runToExit(env)) {
            is Exit.Success -> return@Effect exit.value
            is Exit.Failure -> exit.cause
        }
        val error = cause.failureOrCause() as? Either.Left ?: raise(cause)
        if (!recurrence.next(error.value)) raise(cause)
    }
    @Suppress("UNREACHABLE_CODE")
    error("unreachable")
}

/** `Effect.retry({ times: n })` */
fun <R, E, A> Effect<R, E, A>.retry(times: Int): Effect<R, E, A> = retry(Schedule.recurs(times))

/** `Effect.repeat(schedule)` — run once, then again while the schedule continues. Returns the last value. */
fun <R, E, A> Effect<R, E, A>.repeat(schedule: Schedule<A>): Effect<R, E, A> = Effect { env ->
    val recurrence = schedule.start()
    var last = unsafeRun(env)
    while (recurrence.next(last)) last = unsafeRun(env)
    last
}

/** `Effect.schedule` — run repeatedly in the background-friendly form: first run *after* the first delay. */
fun <R, E, A> Effect<R, E, A>.schedule(schedule: Schedule<Any?>): Effect<R, E, Unit> = Effect { env ->
    val recurrence = schedule.start()
    while (recurrence.next(Unit)) unsafeRun(env)
}
