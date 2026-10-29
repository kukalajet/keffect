package keffect

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.yield
import kotlin.time.Duration

/**
 * A lazy, immutable description of a program that:
 *  - needs an environment of type [R] to run,
 *  - may fail with a typed, expected error of type [E],
 *  - succeeds with a value of type [A].
 *
 * Mirrors Effect-TS's `Effect<A, E, R>` (the parameter order follows Kotlin's
 * variance conventions: `in R, out E, out A`).
 *
 * Nothing runs until you call one of the `run*` functions.
 *
 * Variance does the work that union types do in TypeScript:
 *  - `out E`: an `Effect<R, NotFound, A>` is an `Effect<R, UserError, A>` when `NotFound : UserError`.
 *  - `in R`: an effect that needs only `HasDb` can be used where `AppEnv : HasDb` is available.
 *    An effect needing nothing has `R = Any?` and can be run directly.
 */
class Effect<in R, out E, out A> @PublishedApi internal constructor(
    @PublishedApi internal val unsafeRun: suspend (R) -> A,
) {
    companion object {
        /** `Effect.succeed` */
        fun <A> succeed(value: A): Effect<Any?, Nothing, A> = Effect { value }

        /** `Effect.fail` — an expected, typed failure. */
        fun <E> fail(error: E): Effect<Any?, E, Nothing> = Effect { raise(Cause.Fail(error)) }

        /** `Effect.die` — an unexpected defect. */
        fun die(defect: Throwable): Effect<Any?, Nothing, Nothing> = Effect { throw defect }

        fun dieMessage(message: String): Effect<Any?, Nothing, Nothing> = die(RuntimeException(message))

        /** `Effect.void` */
        val unit: Effect<Any?, Nothing, Unit> = succeed(Unit)

        /** `Effect.sync` — a side effect that is not expected to throw (throws become defects). */
        fun <A> sync(thunk: () -> A): Effect<Any?, Nothing, A> = Effect { thunk() }

        /** `Effect.promise` — a suspending computation that is not expected to throw. */
        fun <A> promise(block: suspend () -> A): Effect<Any?, Nothing, A> = Effect { block() }

        /**
         * `Effect.try` / `Effect.tryPromise` — run a (suspending) block, turning thrown
         * exceptions into typed errors with [catch]. Cancellation is never caught.
         */
        fun <E, A> attempt(catch: (Throwable) -> E, block: suspend () -> A): Effect<Any?, E, A> = Effect {
            try {
                block()
            } catch (c: CancellationException) {
                throw c
            } catch (t: Throwable) {
                raise(Cause.Fail(catch(t)))
            }
        }

        /** `Effect.suspend` — build the effect lazily, each time it runs. */
        fun <R, E, A> defer(thunk: () -> Effect<R, E, A>): Effect<R, E, A> = Effect { env -> thunk().unsafeRun(env) }

        /** Lift a nullable value, failing with [ifNull] when it is null. */
        fun <E, A : Any> fromNullable(value: A?, ifNull: () -> E): Effect<Any?, E, A> =
            Effect { value ?: raise(Cause.Fail(ifNull())) }

        fun sleep(duration: Duration): Effect<Any?, Nothing, Unit> = Effect { delay(duration) }

        val yieldNow: Effect<Any?, Nothing, Unit> = Effect { yield() }

        /** An effect that never completes (until interrupted). */
        val never: Effect<Any?, Nothing, Nothing> = Effect { kotlinx.coroutines.awaitCancellation() }

        /** `Effect.interrupt` — interrupt the current fiber. */
        val interrupt: Effect<Any?, Nothing, Nothing> = Effect { throw CancellationException("Interrupted") }

        /** Access the whole environment. The analogue of yielding a service Tag. */
        fun <R> environment(): Effect<R, Nothing, R> = Effect { it }

        /** Access a part of the environment, e.g. `Effect.service<HasDb, Db> { it.db }`. */
        fun <R, S> service(select: (R) -> S): Effect<R, Nothing, S> = Effect { select(it) }

        /** Access a service and run an effect with it in one step. */
        fun <R, E, A> serviceWithEffect(f: (R) -> Effect<R, E, A>): Effect<R, E, A> =
            Effect { env -> f(env).unsafeRun(env) }

        /** Build an effect from a raw suspending function of the environment. */
        fun <R, E, A> of(block: suspend (R) -> A): Effect<R, E, A> = Effect(block)

        /** Raise a [Cause] as the failure of an effect. */
        fun <E> failCause(cause: Cause<E>): Effect<Any?, E, Nothing> = Effect { raise(cause) }
    }
}

typealias IO<E, A> = Effect<Any?, E, A>
typealias UIO<A> = Effect<Any?, Nothing, A>


/** Why an effect failed. Mirrors Effect-TS `Cause`. */
sealed interface Cause<out E> {
    /** An expected, typed error. */
    data class Fail<out E>(val error: E) : Cause<E>

    /** An unexpected defect (a thrown exception). */
    data class Die(val defect: Throwable) : Cause<Nothing>

    /** The fiber was interrupted. */
    data object Interrupt : Cause<Nothing>

    /** Both sides failed (e.g. in `race`). */
    data class Both<out E>(val left: Cause<E>, val right: Cause<E>) : Cause<E>

    fun pretty(): String = when (this) {
        is Fail -> "Fail($error)"
        is Die -> "Die(${defect::class.simpleName}: ${defect.message})"
        Interrupt -> "Interrupt"
        is Both -> "Both(${left.pretty()}, ${right.pretty()})"
    }
}

/** The result of running an effect. Mirrors Effect-TS `Exit`. */
sealed interface Exit<out E, out A> {
    data class Success<out A>(val value: A) : Exit<Nothing, A>
    data class Failure<out E>(val cause: Cause<E>) : Exit<E, Nothing>

    val isSuccess: Boolean get() = this is Success

    fun getOrNull(): A? = (this as? Success)?.value
}

/** A minimal Either, so the library has no dependencies besides coroutines. */
sealed interface Either<out L, out R> {
    data class Left<out L>(val value: L) : Either<L, Nothing>
    data class Right<out R>(val value: R) : Either<Nothing, R>
}

/** Thrown by [run] / [runBlocking] when an effect does not succeed. */
class FiberFailure(val effectCause: Cause<*>) : RuntimeException(
    "Effect failed: ${effectCause.pretty()}",
    (effectCause as? Cause.Die)?.defect,
)

// ---------------------------------------------------------------------------
// Internal plumbing between "exceptions" and Cause/Exit.
//
// A running effect reports failure by throwing. This section is the only code that
// knows how: [raise] turns a Cause into a throw, [exitOf] turns a throw back into
// a Cause, and the round trip is lossless. Everything else speaks Cause/Exit.
//  - Fail and Both travel inside a [CauseFailure],
//  - Die is the defect itself, so it keeps its stack trace and type,
//  - Interrupt is a CancellationException, because that is how coroutines cancel.
// ---------------------------------------------------------------------------

/** Internal carrier for causes that are not plain throwables. Never escapes the library. Stackless on purpose. */
@PublishedApi
internal class CauseFailure(val failure: Cause<*>) : RuntimeException(null, null, false, false) {
    override val message: String get() = "Unhandled failure: ${failure.pretty()}"
}

/** Run [block] and capture its outcome. Cancellation is rethrown (it is how interruption propagates). */
@PublishedApi
internal suspend fun <E, A> exitOf(block: suspend () -> A): Exit<E, A> =
    try {
        Exit.Success(block())
    } catch (f: CauseFailure) {
        @Suppress("UNCHECKED_CAST")
        Exit.Failure(f.failure as Cause<E>)
    } catch (c: CancellationException) {
        throw c
    } catch (t: Throwable) {
        Exit.Failure(Cause.Die(t))
    }

@PublishedApi
internal suspend fun <R, E, A> Effect<R, E, A>.runToExit(env: R): Exit<E, A> = exitOf { unsafeRun(env) }

/** Re-raise a cause inside a running effect. */
@PublishedApi
internal fun raise(cause: Cause<*>): Nothing = when (cause) {
    is Cause.Die -> throw cause.defect
    Cause.Interrupt -> throw CancellationException("Interrupted")
    is Cause.Fail, is Cause.Both -> throw CauseFailure(cause)
}

/**
 * The primitive every error operator is built on: continue with [onFailure] or [onSuccess]
 * depending on the outcome. Interruption is not a failure here; it keeps propagating.
 */
internal fun <R, E, E2, A, B> Effect<R, E, A>.foldCause(
    onFailure: (Cause<E>) -> Effect<R, E2, B>,
    onSuccess: (A) -> Effect<R, E2, B>,
): Effect<R, E2, B> = Effect { env ->
    when (val exit = runToExit(env)) {
        is Exit.Success -> onSuccess(exit.value).unsafeRun(env)
        is Exit.Failure -> onFailure(exit.cause).unsafeRun(env)
    }
}

/** The first typed error in this cause, or the cause itself when it holds none (so it fits any error type). */
internal fun <E> Cause<E>.failureOrCause(): Either<E, Cause<Nothing>> {
    fun firstFail(c: Cause<E>): Cause.Fail<E>? = when (c) {
        is Cause.Fail -> c
        is Cause.Both -> firstFail(c.left) ?: firstFail(c.right)
        else -> null
    }
    @Suppress("UNCHECKED_CAST")
    return firstFail(this)?.let { Either.Left(it.error) } ?: Either.Right(this as Cause<Nothing>)
}

/** Map every typed error in this cause. */
internal fun <E, E2> Cause<E>.mapFail(f: (E) -> E2): Cause<E2> = when (this) {
    is Cause.Fail -> Cause.Fail(f(error))
    is Cause.Die -> this
    Cause.Interrupt -> Cause.Interrupt
    is Cause.Both -> Cause.Both(left.mapFail(f), right.mapFail(f))
}

@PublishedApi
internal fun <E, A> Exit<E, A>.getOrRaise(): A = when (this) {
    is Exit.Success -> value
    is Exit.Failure -> raise(cause)
}

/** Turn a caught interruption into an Exit, unless the *caller* itself was cancelled. */
internal suspend fun interruptedExit(c: CancellationException): Exit<Nothing, Nothing> {
    currentCoroutineContext().ensureActive() // outer cancellation: keep propagating
    return Exit.Failure(Cause.Interrupt)
}
