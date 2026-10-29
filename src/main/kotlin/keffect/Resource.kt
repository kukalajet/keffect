package keffect

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * `Scope` — collects finalizers and runs them in reverse order when closed.
 *
 * Effect-TS tracks `Scope` in the requirements type. Here it lives in the coroutine
 * context instead: every `run*` call opens a root scope, and [Effect.Companion.scoped]
 * opens a nested one. Simpler, at the price of not being visible in the type.
 */
class Scope internal constructor() {
    private val lock = Mutex()
    private val finalizers = ArrayDeque<suspend (Exit<*, *>) -> Unit>()
    private var closed = false

    internal suspend fun addFinalizer(finalizer: suspend (Exit<*, *>) -> Unit) {
        val runNow = lock.withLock {
            if (closed) true else { finalizers.addFirst(finalizer); false }
        }
        if (runNow) withContext(NonCancellable) { finalizer(Exit.Failure(Cause.Interrupt)) }
    }

    /** Run all finalizers (last acquired, first released). Errors in finalizers become defects. */
    internal suspend fun close(exit: Exit<*, *>) {
        val toRun = lock.withLock {
            closed = true
            finalizers.toList().also { finalizers.clear() }
        }
        withContext(NonCancellable) {
            var firstError: Throwable? = null
            for (f in toRun) {
                try {
                    f(exit)
                } catch (t: Throwable) {
                    if (firstError == null) firstError = t
                }
            }
            firstError?.let { throw it }
        }
    }
}

internal class ScopeElement(val scope: Scope) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ScopeElement>
}

private suspend fun currentScope(): Scope =
    currentCoroutineContext()[ScopeElement]?.scope
        ?: error("No Scope in context. Run the effect with run*/runBlocking, or wrap it in Effect.scoped { }.")

/** Run [block] in a fresh scope and close it with the outcome. */
internal suspend fun <T> withScope(block: suspend () -> T): T {
    val scope = Scope()
    val result = try {
        withContext(ScopeElement(scope)) { block() }
    } catch (c: CancellationException) {
        scope.close(Exit.Failure(Cause.Interrupt))
        throw c
    } catch (t: TypedFailure) {
        scope.close(Exit.Failure(Cause.Fail(t.error)))
        throw t
    } catch (t: Throwable) {
        scope.close(Exit.Failure(Cause.Die(t)))
        throw t
    }
    scope.close(Exit.Success(result))
    return result
}

/**
 * `Effect.acquireRelease` (exit-aware form) — acquire a resource (uninterruptibly) and register its release
 * in the current scope. The release receives the scope's exit.
 */
fun <R, E, A> Effect.Companion.acquireReleaseExit(
    acquire: Effect<R, E, A>,
    release: (A, Exit<*, *>) -> Effect<R, Nothing, *>,
): Effect<R, E, A> = Effect { env ->
    val scope = currentScope()
    withContext(NonCancellable) {
        val resource = acquire.unsafeRun(env)
        scope.addFinalizer { exit -> release(resource, exit).unsafeRun(env) }
        resource
    }
}

/** `Effect.acquireRelease` when the release doesn't care how the scope ended. */
fun <R, E, A> Effect.Companion.acquireRelease(
    acquire: Effect<R, E, A>,
    release: (A) -> Effect<R, Nothing, *>,
): Effect<R, E, A> = acquireReleaseExit(acquire) { a, _ -> release(a) }

/** `Effect.addFinalizer` */
fun <R> Effect.Companion.addFinalizer(finalizer: (Exit<*, *>) -> Effect<R, Nothing, *>): Effect<R, Nothing, Unit> =
    Effect { env -> currentScope().addFinalizer { exit -> finalizer(exit).unsafeRun(env) } }

/** `Effect.scoped` — run [effect] in its own scope; its resources are released when it ends. */
fun <R, E, A> Effect.Companion.scoped(effect: Effect<R, E, A>): Effect<R, E, A> =
    Effect { env -> withScope { effect.unsafeRun(env) } }

/** Builder form: `Effect.scoped { val f = acquireRelease(...).bind(); ... }` */
fun <R, E, A> Effect.Companion.scoped(block: suspend EffectScope<R, E>.() -> A): Effect<R, E, A> =
    scoped(effect(block))

/** `Effect.acquireUseRelease` — bracket without an outer scope. */
fun <R, E, A, B> Effect.Companion.acquireUseRelease(
    acquire: Effect<R, E, A>,
    use: (A) -> Effect<R, E, B>,
    release: (A, Exit<E, B>) -> Effect<R, Nothing, *>,
): Effect<R, E, B> = Effect { env ->
    val resource = withContext(NonCancellable) { acquire.unsafeRun(env) }
    val exit = try {
        use(resource).runToExit(env)
    } catch (c: CancellationException) {
        withContext(NonCancellable) { release(resource, Exit.Failure(Cause.Interrupt)).unsafeRun(env) }
        throw c
    }
    withContext(NonCancellable) { release(resource, exit).unsafeRun(env) }
    exit.getOrRaise()
}

/** `Effect.forkScoped` — a fiber that lives as long as the current scope, not the current fiber. */
fun <R, E, A> Effect<R, E, A>.forkScoped(): Effect<R, Nothing, Fiber<E, A>> = Effect { env ->
    val fiber = forkDaemon().unsafeRun(env)
    currentScope().addFinalizer { fiber.interrupt.unsafeRun(Unit) }
    fiber
}
