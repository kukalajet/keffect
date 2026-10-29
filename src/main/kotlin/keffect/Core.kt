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
        fun <E> fail(error: E): Effect<Any?, E, Nothing> = Effect { throw TypedFailure(error) }

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
                throw TypedFailure(catch(t))
            }
        }

        /** `Effect.suspend` — build the effect lazily, each time it runs. */
        fun <R, E, A> defer(thunk: () -> Effect<R, E, A>): Effect<R, E, A> = Effect { env -> thunk().unsafeRun(env) }

        /** Lift a nullable value, failing with [ifNull] when it is null. */
        fun <E, A : Any> fromNullable(value: A?, ifNull: () -> E): Effect<Any?, E, A> =
            Effect { value ?: throw TypedFailure(ifNull()) }

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

/** Internal carrier for typed errors. Never escapes the library. Stackless on purpose. */
@PublishedApi
internal class TypedFailure(val error: Any?) : RuntimeException(null, null, false, false) {
    override val message: String get() = "Unhandled typed failure: $error"
}

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
// Internal plumbing between "exceptions" and Cause/Exit
// ---------------------------------------------------------------------------

/** Run an effect and capture its outcome. Cancellation is rethrown (it is how interruption propagates). */
@PublishedApi
internal suspend fun <R, E, A> Effect<R, E, A>.runToExit(env: R): Exit<E, A> =
    try {
        Exit.Success(unsafeRun(env))
    } catch (f: TypedFailure) {
        @Suppress("UNCHECKED_CAST")
        Exit.Failure(Cause.Fail(f.error as E))
    } catch (c: CancellationException) {
        throw c
    } catch (t: Throwable) {
        Exit.Failure(Cause.Die(t))
    }

/** Re-raise a cause inside a running effect. */
@PublishedApi
internal fun raise(cause: Cause<*>): Nothing = when (cause) {
    is Cause.Fail -> throw TypedFailure(cause.error)
    is Cause.Die -> throw cause.defect
    Cause.Interrupt -> throw CancellationException("Interrupted")
    is Cause.Both -> raise(cause.left)
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
