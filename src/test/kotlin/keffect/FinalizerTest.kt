package keffect

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

// Finalizers run whatever happens, and a failing finalizer never hides the original outcome:
// it is appended to it as `Cause.Then(original, finalizerFailure)`.
class FinalizerTest {
    private val boom = IllegalStateException("boom")
    private val bang = IllegalStateException("bang")
    private val log = mutableListOf<String>()
    private fun record(line: String) = Effect.sync { log += line }

    @Test
    fun `a failing finalizer after success is the failure`() {
        assertEquals(Exit.Failure(Cause.Die(boom)), Effect.succeed(1).ensuring(Effect.die(boom)).runBlockingExit())
    }

    @Test
    fun `a failing finalizer after a failure keeps both causes`() {
        assertEquals(
            Exit.Failure(Cause.Then(Cause.Fail("e"), Cause.Die(boom))),
            Effect.fail("e").ensuring(Effect.die(boom)).runBlockingExit(),
        )
    }

    @Test
    fun `a failing finalizer after interruption keeps the interruption`() {
        assertEquals(
            Exit.Failure(Cause.Then(Cause.Interrupt, Cause.Die(boom))),
            Effect.interrupt.onExit { Effect.die(boom) }.runBlockingExit(),
        )
    }

    @Test
    fun `acquireUseRelease appends a failing release to the use failure`() {
        val eff: IO<String, Int> = Effect.acquireUseRelease(
            acquire = Effect.succeed("res"),
            use = { Effect.fail("e") },
            release = { _, _ -> Effect.die(boom) },
        )
        assertEquals(Exit.Failure(Cause.Then(Cause.Fail("e"), Cause.Die(boom))), eff.runBlockingExit())
    }

    @Test
    fun `acquireUseRelease releases when building the use effect throws`() {
        val eff: IO<String, Int> = Effect.acquireUseRelease(
            acquire = Effect.succeed("res"),
            use = { throw boom },
            release = { _, _ -> record("released") },
        )
        assertEquals(Exit.Failure(Cause.Die(boom)), eff.runBlockingExit())
        assertEquals(listOf("released"), log)
    }

    @Test
    fun `a scope runs every finalizer and keeps every failure in release order`() {
        val eff: IO<String, Int> = Effect.scoped {
            Effect.addFinalizer<Any?> { record("first acquired") }.bind()
            Effect.addFinalizer<Any?> { Effect.die(boom) }.bind()
            Effect.addFinalizer<Any?> { Effect.die(bang) }.bind()
            fail("e")
        }
        assertEquals(
            Exit.Failure(Cause.Then(Cause.Fail("e"), Cause.Then(Cause.Die(bang), Cause.Die(boom)))),
            eff.runBlockingExit(),
        )
        assertEquals(listOf("first acquired"), log)
    }

    @Test
    fun `a failing root finalizer is appended to the run's failure`() = runTest {
        val exit = Effect.addFinalizer<Any?> { Effect.die(boom) }.andThen(Effect.fail("e")).runExit()
        assertEquals(Exit.Failure(Cause.Then(Cause.Fail("e"), Cause.Die(boom))), exit)
    }

    @Test
    fun `catchAll finds the typed error inside Then`() {
        val recovered = Effect.fail("e").ensuring(Effect.die(boom)).catchAll { Effect.succeed(it) }
        assertEquals("e", recovered.runBlocking())
    }

    @Test
    fun `an interrupted fiber reports a finalizer that failed during interruption`() = runTest {
        val dying: UIO<Nothing> = Effect.never.ensuring(Effect.die(boom))
        // Also when the effect runs in a scope of its own.
        for (eff in listOf(dying, Effect.scoped(dying), dying.provide(Layer.succeed(Unit)))) {
            val exit = effect<Any?, Nothing, Exit<Nothing, Nothing>> {
                val fiber = eff.forkDaemon().bind()
                Effect.yieldNow.bind()
                fiber.interrupt.bind()
            }.run()
            assertEquals(Exit.Failure(Cause.Then(Cause.Interrupt, Cause.Die(boom))), exit)
        }
    }
}
