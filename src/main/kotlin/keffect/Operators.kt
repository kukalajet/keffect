package keffect

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

// All operators are extension functions with a single R / E / A per call site.
// Thanks to `in R` / `out E` / `out A`, Kotlin infers the widest error type and the
// narrowest environment type that both sides accept, e.g. combining an
// Effect<HasDb, NotFound, _> with an Effect<HasDb, DbError, _> gives
// Effect<HasDb, UserError, _> when both errors extend `sealed interface UserError`.

// ---- Transforming success ---------------------------------------------------

fun <R, E, A, B> Effect<R, E, A>.map(f: (A) -> B): Effect<R, E, B> =
    Effect { env -> f(unsafeRun(env)) }

fun <R, E, A, B> Effect<R, E, A>.flatMap(f: (A) -> Effect<R, E, B>): Effect<R, E, B> =
    Effect { env -> f(unsafeRun(env)).unsafeRun(env) }

/** `Effect.andThen` with an effect: run this, then [that], keep the second result. */
fun <R, E, A, B> Effect<R, E, A>.andThen(that: Effect<R, E, B>): Effect<R, E, B> =
    Effect { env -> unsafeRun(env); that.unsafeRun(env) }

/** Run [f] for its effect, keep the original value. */
fun <R, E, A> Effect<R, E, A>.tap(f: (A) -> Effect<R, E, *>): Effect<R, E, A> =
    Effect { env -> unsafeRun(env).also { f(it).unsafeRun(env) } }

fun <R, E, A, B> Effect<R, E, A>.asValue(value: B): Effect<R, E, B> = map { value }

fun <R, E, A> Effect<R, E, A>.asUnit(): Effect<R, E, Unit> = map { }

fun <R, E, A, B> Effect<R, E, A>.zip(that: Effect<R, E, B>): Effect<R, E, Pair<A, B>> =
    Effect { env -> unsafeRun(env) to that.unsafeRun(env) }

fun <R, E, A, B, C> Effect<R, E, A>.zipWith(that: Effect<R, E, B>, f: (A, B) -> C): Effect<R, E, C> =
    Effect { env -> f(unsafeRun(env), that.unsafeRun(env)) }

fun <R, E, A> Effect<R, E, A>.delay(duration: Duration): Effect<R, E, A> =
    Effect { env -> kotlinx.coroutines.delay(duration); unsafeRun(env) }

// ---- Error handling ----------------------------------------------------------

fun <R, E, E2, A> Effect<R, E, A>.mapError(f: (E) -> E2): Effect<R, E2, A> =
    foldCause({ Effect.failCause(it.mapFail(f)) }, { Effect.succeed(it) })

/** `Effect.catchAll` — recover from every typed error. Defects and interruption pass through. */
fun <R, E, E2, A> Effect<R, E, A>.catchAll(f: (E) -> Effect<R, E2, A>): Effect<R, E2, A> =
    foldCause({ cause ->
        when (val e = cause.failureOrCause()) {
            is Either.Left -> f(e.value)
            is Either.Right -> Effect.failCause(e.value)
        }
    }, { Effect.succeed(it) })

/**
 * `Effect.catchSome` / `catchTag` — recover from some errors. Return `null` to re-raise.
 *
 * ```
 * eff.catchSome { e -> if (e is UserError.NotFound) Effect.succeed(Guest) else null }
 * ```
 */
fun <R, E, A> Effect<R, E, A>.catchSome(f: (E) -> Effect<R, E, A>?): Effect<R, E, A> =
    foldCause({ cause ->
        when (val e = cause.failureOrCause()) {
            is Either.Left -> f(e.value) ?: Effect.failCause(cause)
            is Either.Right -> Effect.failCause(cause)
        }
    }, { Effect.succeed(it) })

/** Recover from one specific error subtype: `eff.catchTag<UserError.NotFound, _, _, _> { ... }` is ugly in
 *  Kotlin, so this takes the class instead: `eff.catchTag(UserError.NotFound::class) { ... }`. */
fun <R, E, A, T : Any> Effect<R, E, A>.catchTag(
    tag: kotlin.reflect.KClass<T>,
    f: (T) -> Effect<R, E, A>,
): Effect<R, E, A> = catchSome { e -> if (tag.isInstance(e)) f(tag.java.cast(e)) else null }

/** Also intercept defects. */
fun <R, E, A> Effect<R, E, A>.catchAllCause(f: (Cause<E>) -> Effect<R, E, A>): Effect<R, E, A> =
    foldCause(f) { Effect.succeed(it) }

fun <R, E, E2, A> Effect<R, E, A>.orElse(that: Effect<R, E2, A>): Effect<R, E2, A> = catchAll { that }

fun <R, E, A> Effect<R, E, A>.orElseSucceed(value: A): Effect<R, Nothing, A> = catchAll { Effect.succeed(value) }

/** Turn typed errors into defects: "this can't happen". */
fun <R, E, A> Effect<R, E, A>.orDie(): Effect<R, Nothing, A> =
    catchAll { e -> Effect.die((e as? Throwable) ?: RuntimeException("orDie: $e")) }

/** Move the error into the success channel. */
fun <R, E, A> Effect<R, E, A>.either(): Effect<R, Nothing, Either<E, A>> =
    map<R, E, A, Either<E, A>> { Either.Right(it) }.catchAll { Effect.succeed(Either.Left(it)) }

/** Capture the full outcome (success, failure, defect) as a value. Interruption still propagates. */
fun <R, E, A> Effect<R, E, A>.exit(): Effect<R, Nothing, Exit<E, A>> = Effect { env -> runToExit(env) }

fun <R, E, A> Effect<R, E, A>.tapError(f: (E) -> Effect<R, Nothing, *>): Effect<R, E, A> =
    foldCause({ cause ->
        when (val e = cause.failureOrCause()) {
            is Either.Left -> f(e.value).andThen(Effect.failCause(cause))
            is Either.Right -> Effect.failCause(cause)
        }
    }, { Effect.succeed(it) })

/** Fail with [error] when [predicate] does not hold. */
fun <R, E, A> Effect<R, E, A>.filterOrFail(predicate: (A) -> Boolean, error: (A) -> E): Effect<R, E, A> =
    flatMap { if (predicate(it)) Effect.succeed(it) else Effect.fail(error(it)) }

// ---- Finalization ------------------------------------------------------------

/** `Effect.ensuring` — always run [finalizer], even on failure or interruption. */
fun <R, E, A> Effect<R, E, A>.ensuring(finalizer: Effect<R, Nothing, *>): Effect<R, E, A> = Effect { env ->
    try {
        unsafeRun(env)
    } finally {
        withContext(NonCancellable) { finalizer.unsafeRun(env) }
    }
}

/** `Effect.onExit` — run [f] with the outcome, uninterruptibly. */
fun <R, E, A> Effect<R, E, A>.onExit(f: (Exit<E, A>) -> Effect<R, Nothing, *>): Effect<R, E, A> = Effect { env ->
    val exit = try {
        runToExit(env)
    } catch (c: CancellationException) {
        withContext(NonCancellable) { f(Exit.Failure(Cause.Interrupt)).unsafeRun(env) }
        throw c
    }
    withContext(NonCancellable) { f(exit).unsafeRun(env) }
    exit.getOrRaise()
}

fun <R, E, A> Effect<R, E, A>.onInterrupt(f: Effect<R, Nothing, *>): Effect<R, E, A> =
    onExit { exit ->
        if (exit is Exit.Failure && exit.cause == Cause.Interrupt) f else Effect.unit
    }

/** Run without being interruptible (e.g. a critical section). */
fun <R, E, A> Effect<R, E, A>.uninterruptible(): Effect<R, E, A> =
    Effect { env -> withContext(NonCancellable) { unsafeRun(env) } }

// ---- Time --------------------------------------------------------------------

private class Box<A>(val value: A)

/** `Effect.timeout` — `null` if it didn't finish in time (and the work is interrupted). */
fun <R, E, A> Effect<R, E, A>.timeout(duration: Duration): Effect<R, E, A?> = Effect { env ->
    withTimeoutOrNull(duration) { Box(unsafeRun(env)) }?.value
}

/** `Effect.timeoutFail` */
fun <R, E, A> Effect<R, E, A>.timeoutFail(duration: Duration, onTimeout: () -> E): Effect<R, E, A> = Effect { env ->
    val box = withTimeoutOrNull(duration) { Box(unsafeRun(env)) }
    box?.value ?: raise(Cause.Fail(onTimeout()))
}

/** Measure how long the effect takes. */
fun <R, E, A> Effect<R, E, A>.timed(): Effect<R, E, Pair<Duration, A>> = Effect { env ->
    val mark = currentTimeSource().markNow()
    val a = unsafeRun(env)
    mark.elapsedNow() to a
}

// ---- Environment -------------------------------------------------------------

/** Satisfy the requirements directly with a value. */
fun <R, E, A> Effect<R, E, A>.provide(env: R): Effect<Any?, E, A> = Effect { unsafeRun(env) }

/** Adapt the environment, e.g. to run a sub-program that needs only part of it. */
fun <R, R2, E, A> Effect<R, E, A>.provideSome(f: (R2) -> R): Effect<R2, E, A> = Effect { env -> unsafeRun(f(env)) }
