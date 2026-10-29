package keffect

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

// ---------------------------------------------------------------------------
// Fibers
//
// A fiber is a coroutine running an effect. Like Effect-TS (and unlike plain
// `launch`), a fiber started with `fork` is *supervised by its parent fiber*:
// when the parent fiber finishes, its still-running children are interrupted.
// `forkDaemon` fibers outlive their parent.
// ---------------------------------------------------------------------------

private val fiberIds = AtomicInteger(0)

internal class FiberContext(val id: Int) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<FiberContext>

    private val children: MutableSet<Job> = ConcurrentHashMap.newKeySet()

    /** Interrupt [child] when this fiber ends (unless it has finished by then). */
    fun supervise(child: Job) {
        children.add(child)
        child.invokeOnCompletion { children.remove(child) }
    }

    suspend fun interruptChildren() = children.forEach { it.cancelAndJoin() }
}

internal suspend fun currentFiberId(): Int = currentCoroutineContext()[FiberContext]?.id ?: 0

/** Run [block] as a fiber: it gets its own id, and children it forks are interrupted when it ends. */
internal suspend fun <T> asFiber(id: Int = fiberIds.incrementAndGet(), block: suspend () -> T): T {
    val fiber = FiberContext(id)
    return withContext(fiber) {
        try {
            block()
        } finally {
            withContext(NonCancellable) { fiber.interruptChildren() }
        }
    }
}

/**
 * A handle to a running effect.
 *
 * The outcome is handed over through [exit] rather than as the result of [job]: a cancelled
 * coroutine discards its result, which would lose e.g. a finalizer that failed during interruption.
 */
class Fiber<out E, out A> internal constructor(
    val id: Int,
    private val job: Job,
    private val exit: Deferred<Exit<E, A>>,
) {
    /** Wait for the fiber and get its outcome as a value. */
    val await: Effect<Any?, Nothing, Exit<E, A>> = Effect { exit.await() }

    /** Wait for the fiber; its failure becomes our failure. */
    val join: Effect<Any?, E, A> = Effect { exit.await().getOrRaise() }

    /** Interrupt the fiber and wait for it to finish its finalizers. */
    val interrupt: Effect<Any?, Nothing, Exit<E, A>> = Effect {
        job.cancelAndJoin()
        exit.await()
    }

    override fun toString() = "Fiber#$id"
}

private suspend fun <R, E, A> startFiber(effect: Effect<R, E, A>, env: R, daemon: Boolean): Fiber<E, A> {
    val ctx = currentCoroutineContext()
    // A fresh Job: the parent coroutine does not wait for it (fibers are not structured like `launch`),
    // but we register it with the parent fiber so it is interrupted when the parent ends.
    val scope = CoroutineScope(ctx.minusKey(Job).minusKey(FiberContext) + Job())
    val id = fiberIds.incrementAndGet()
    val exit = CompletableDeferred<Exit<E, A>>()
    val job = scope.launch {
        asFiber(id) {
            exit.complete(
                try {
                    effect.runToExit(env)
                } catch (c: CancellationException) {
                    Exit.Failure(Cause.Interrupt)
                },
            )
        }
    }
    // Interrupted before it started: there is no other outcome.
    job.invokeOnCompletion { exit.complete(Exit.Failure(Cause.Interrupt)) }
    if (!daemon) ctx[FiberContext]?.supervise(job)
    return Fiber(id, job, exit)
}

/** Start [block] as a child fiber of this structured scope: it fails with the scope and is joined by it. */
private fun <T> CoroutineScope.asyncFiber(block: suspend () -> T): Deferred<T> = async { asFiber(block = block) }

/** `Effect.fork` — start the effect in a new fiber, supervised by the current one. */
fun <R, E, A> Effect<R, E, A>.fork(): Effect<R, Nothing, Fiber<E, A>> =
    Effect { env -> startFiber(this, env, daemon = false) }

/** `Effect.forkDaemon` — start the effect in a fiber that outlives the current one. */
fun <R, E, A> Effect<R, E, A>.forkDaemon(): Effect<R, Nothing, Fiber<E, A>> =
    Effect { env -> startFiber(this, env, daemon = true) }

// ---------------------------------------------------------------------------
// Parallel combinators. These use structured concurrency: the first failure
// interrupts the siblings, and nothing leaks past the call.
// ---------------------------------------------------------------------------

/** Concurrency option, like `{ concurrency: n | "unbounded" }`. */
sealed interface Concurrency {
    data object Sequential : Concurrency
    data object Unbounded : Concurrency
    data class Bounded(val n: Int) : Concurrency

    companion object {
        operator fun invoke(n: Int): Concurrency = if (n <= 1) Sequential else Bounded(n)
    }
}

/** `Effect.forEach(items, f, { concurrency })` */
fun <R, E, T, A> Effect.Companion.forEach(
    items: Iterable<T>,
    concurrency: Concurrency = Concurrency.Sequential,
    f: (T) -> Effect<R, E, A>,
): Effect<R, E, List<A>> = Effect { env ->
    when (concurrency) {
        Concurrency.Sequential -> items.map { f(it).unsafeRun(env) }
        Concurrency.Unbounded -> coroutineScope {
            items.map { item -> asyncFiber { f(item).unsafeRun(env) } }.awaitAll()
        }

        is Concurrency.Bounded -> coroutineScope {
            val permits = Semaphore(concurrency.n)
            items.map { item -> asyncFiber { permits.withPermit { f(item).unsafeRun(env) } } }.awaitAll()
        }
    }
}

/** `Effect.all([...], { concurrency })` */
fun <R, E, A> Effect.Companion.all(
    effects: Iterable<Effect<R, E, A>>,
    concurrency: Concurrency = Concurrency.Sequential,
): Effect<R, E, List<A>> = forEach(effects, concurrency) { it }

fun <R, E, A> Effect.Companion.all(
    vararg effects: Effect<R, E, A>,
    concurrency: Concurrency = Concurrency.Sequential,
): Effect<R, E, List<A>> = all(effects.asList(), concurrency)

/** Run both concurrently; fail fast if either fails. */
fun <R, E, A, B> Effect<R, E, A>.zipPar(that: Effect<R, E, B>): Effect<R, E, Pair<A, B>> = Effect { env ->
    coroutineScope {
        val a = asyncFiber { unsafeRun(env) }
        val b = asyncFiber { that.unsafeRun(env) }
        a.await() to b.await()
    }
}

fun <R, E, A, B, C> Effect<R, E, A>.zipWithPar(that: Effect<R, E, B>, f: (A, B) -> C): Effect<R, E, C> =
    zipPar(that).map { (a, b) -> f(a, b) }

/**
 * `Effect.race` — the first effect to *succeed* wins and the loser is interrupted.
 * If both fail, the race fails with both causes.
 */
fun <R, E, A> Effect<R, E, A>.race(that: Effect<R, E, A>): Effect<R, E, A> = Effect { env ->
    coroutineScope {
        val left = asyncFiber { runToExit(env) }
        val right = asyncFiber { that.runToExit(env) }
        val (first, other) = select<Pair<Exit<E, A>, Deferred<Exit<E, A>>>> {
            left.onAwait { it to right }
            right.onAwait { it to left }
        }
        when (first) {
            is Exit.Success -> {
                other.cancelAndJoin()
                first.value
            }

            is Exit.Failure -> when (val second = other.await()) {
                is Exit.Success -> second.value
                is Exit.Failure -> raise(Cause.Both(first.cause, second.cause))
            }
        }
    }
}

fun <R, E, A> Effect.Companion.raceAll(effects: List<Effect<R, E, A>>): Effect<R, E, A> =
    effects.reduce { acc, eff -> acc.race(eff) }
