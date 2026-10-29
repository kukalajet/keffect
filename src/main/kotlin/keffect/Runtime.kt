package keffect

import kotlinx.coroutines.CancellationException

// Running effects. Only effects whose requirements are fully satisfied (`R = Any?`)
// can be run — `provide` them first. The compiler enforces this.

/** `Effect.runPromiseExit` — run as the root fiber and return the outcome. */
suspend fun <E, A> Effect<Any?, E, A>.runExit(): Exit<E, A> =
    try {
        asFiber { scopedExit { unsafeRun(Unit) } }
    } catch (c: CancellationException) {
        interruptedExit(c)
    } catch (t: Throwable) {
        // A failing finalizer while closing the root scope.
        Exit.Failure(Cause.Die(t))
    }

/** `Effect.runPromise` — run and return the value, or throw [FiberFailure]. */
suspend fun <E, A> Effect<Any?, E, A>.run(): A = when (val exit = runExit()) {
    is Exit.Success -> exit.value
    is Exit.Failure -> throw FiberFailure(exit.cause)
}

/** `Effect.runSync`-ish: block the current thread. Handy in `main` and tests. */
fun <E, A> Effect<Any?, E, A>.runBlocking(): A = kotlinx.coroutines.runBlocking { run() }

fun <E, A> Effect<Any?, E, A>.runBlockingExit(): Exit<E, A> = kotlinx.coroutines.runBlocking { runExit() }
