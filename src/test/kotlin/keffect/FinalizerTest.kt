package keffect

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

// Finalizers run whatever happens, and a failing finalizer never hides the original outcome:
// it is appended to it as `Cause.Then(original, finalizerFailure)`.
// Causes are compared via `pretty()`: coroutines' stack-trace recovery may hand back a copy of a defect.
class FinalizerTest {
    private val boom = IllegalStateException("boom")
    private val bang = IllegalStateException("bang")
    private val log = mutableListOf<String>()
    private fun record(line: String) = Effect.sync { log += line }

    private fun Exit<*, *>.pretty(): String = when (this) {
        is Exit.Success -> "Success($value)"
        is Exit.Failure -> cause.pretty()
    }

    @Test
    fun `a failing finalizer after success is the failure`() {
        assertEquals("Die(IllegalStateException: boom)", Effect.succeed(1).ensuring(Effect.die(boom)).runBlockingExit().pretty())
    }

    @Test
    fun `a failing finalizer after a failure keeps both causes`() {
        assertEquals(
            "Then(Fail(e), Die(IllegalStateException: boom))",
            Effect.fail("e").ensuring(Effect.die(boom)).runBlockingExit().pretty(),
        )
    }

    @Test
    fun `a failing finalizer after interruption keeps the interruption`() {
        assertEquals(
            "Then(Interrupt, Die(IllegalStateException: boom))",
            Effect.interrupt.onExit { Effect.die(boom) }.runBlockingExit().pretty(),
        )
    }

    @Test
    fun `acquireUseRelease appends a failing release to the use failure`() {
        val eff: IO<String, Int> = Effect.acquireUseRelease(
            acquire = Effect.succeed("res"),
            use = { Effect.fail("e") },
            release = { _, _ -> Effect.die(boom) },
        )
        assertEquals("Then(Fail(e), Die(IllegalStateException: boom))", eff.runBlockingExit().pretty())
    }

    @Test
    fun `acquireUseRelease releases when building the use effect throws`() {
        val eff: IO<String, Int> = Effect.acquireUseRelease(
            acquire = Effect.succeed("res"),
            use = { throw boom },
            release = { _, _ -> record("released") },
        )
        assertEquals("Die(IllegalStateException: boom)", eff.runBlockingExit().pretty())
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
            "Then(Fail(e), Then(Die(IllegalStateException: bang), Die(IllegalStateException: boom)))",
            eff.runBlockingExit().pretty(),
        )
        assertEquals(listOf("first acquired"), log)
    }

    @Test
    fun `a failing root finalizer is appended to the run's failure`() = runTest {
        val exit = Effect.addFinalizer<Any?> { Effect.die(boom) }.andThen(Effect.fail("e")).runExit()
        assertEquals("Then(Fail(e), Die(IllegalStateException: boom))", exit.pretty())
    }

    @Test
    fun `catchAll finds the typed error inside Then`() {
        val recovered = Effect.fail("e").ensuring(Effect.die(boom)).catchAll { Effect.succeed(it) }
        assertEquals("e", recovered.runBlocking())
    }

    @Test
    fun `an interrupted fiber reports a finalizer that failed during interruption`() = runTest {
        val exit = effect<Any?, Nothing, Exit<Nothing, Nothing>> {
            val fiber = Effect.never.ensuring(Effect.die(boom)).forkDaemon().bind()
            Effect.yieldNow.bind()
            fiber.interrupt.bind()
        }.run()
        assertEquals("Then(Interrupt, Die(IllegalStateException: boom))", exit.pretty())
    }
}
