package keffect

/**
 * The receiver of [effect] / `Effect.gen` blocks. Plays the role of `yield*` in Effect-TS:
 * call [bind] on any effect whose environment and error fit, and you get its value back
 * (or the block short-circuits with its failure).
 */
class EffectScope<R, E> @PublishedApi internal constructor(
    /** The environment this program runs in — the analogue of yielding service Tags. */
    val env: R,
) {
    /** `yield* eff` */
    suspend fun <A> Effect<R, E, A>.bind(): A = unsafeRun(env)

    /** Shorthand for [bind]: `val user = !getUser(1)`. */
    suspend operator fun <A> Effect<R, E, A>.not(): A = unsafeRun(env)

    /** Build the layer (memoized) and get its output. */
    suspend fun <A> Layer<R, E, A>.bind(): A = build().unsafeRun(env)

    /** `yield* Effect.fail(error)` */
    fun fail(error: E): Nothing = raise(Cause.Fail(error))

    /** Fail with [error] unless [condition] holds. */
    inline fun ensure(condition: Boolean, error: () -> E) {
        if (!condition) fail(error())
    }

    /** Unwrap a nullable value or fail. */
    inline fun <A : Any> A?.orFail(error: () -> E): A = this ?: fail(error())

    /** Unwrap an Either. */
    fun <A> Either<E, A>.bind(): A = when (this) {
        is Either.Left -> fail(value)
        is Either.Right -> value
    }
}

/**
 * `Effect.gen` — write effectful code in direct style.
 *
 * ```
 * val program: Effect<AppEnv, AppError, User> = effect {
 *     val user = env.users.find(id).bind()
 *     ensure(user.active) { AppError.Inactive(id) }
 *     user
 * }
 * ```
 *
 * Annotate the result type (or the type arguments) so Kotlin knows R and E.
 */
fun <R, E, A> effect(block: suspend EffectScope<R, E>.() -> A): Effect<R, E, A> =
    Effect { env -> EffectScope<R, E>(env).block() }

/** Same as [effect], spelled like Effect-TS. */
fun <R, E, A> Effect.Companion.gen(block: suspend EffectScope<R, E>.() -> A): Effect<R, E, A> = effect(block)
