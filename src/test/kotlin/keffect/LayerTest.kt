package keffect

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

class LayerTest {
    private val boom = IllegalStateException("boom")
    private val log = mutableListOf<String>()
    private fun record(line: String) = Effect.sync { log += line }

    /** A layer that logs its build, and its release with how the scope ended. */
    private fun resource(name: String): Layer<Any?, Nothing, String> = Layer.effect(
        Effect.acquireReleaseExit(record("acquire $name").asValue(name)) { _, exit ->
            record("release $name ${if (exit.isSuccess) "ok" else "failed"}")
        },
    )

    private val use: Effect<String, Nothing, String> = Effect.environment<String>().tap { record("use $it") }

    @Test
    fun `a layer referenced twice in one graph is built once`() = runTest {
        val shared = resource("db")
        val app = shared.zipWith(shared) { a, b -> "$a+$b" }
        assertEquals("db+db", use.provide(app).run())
        assertEquals(listOf("acquire db", "use db+db", "release db ok"), log)
    }

    @Test
    fun `a layer shared in the graph is built once per provide`() = runTest {
        var builds = 0
        val shared: Layer<Any?, Nothing, Int> = Layer.effect(Effect.sync { ++builds })
        val both = shared.zipWith(shared.map { it * 10 }) { a, b -> a + b }
        assertEquals(11, Effect.environment<Int>().provide(both).run())
        assertEquals(1, builds)
    }

    @Test
    fun `a shared layer is built once even when built concurrently`() = runTest {
        val shared = resource("db")
        val slow = shared.andThen(Layer.effect(Effect.environment<String>().delay(10.milliseconds)))
        val app = slow.zipWithPar(shared) { a, b -> "$a+$b" }
        use.provide(app).run()
        assertEquals(1, log.count { it == "acquire db" })
    }

    @Test
    fun `each provide builds its layers afresh`() = runTest {
        val db = resource("db")
        use.provide(db).andThen(use.provide(db)).run()
        assertEquals(
            listOf("acquire db", "use db", "release db ok", "acquire db", "use db", "release db ok"),
            log,
        )
    }

    @Test
    fun `layer resources are released after the effect, last acquired first`() = runTest {
        val app = resource("a").andThen(Layer.effect(Effect.environment<String>()))
            .zipWith(resource("b")) { a, b -> a + b }
        use.provide(app).run()
        assertEquals(listOf("acquire a", "acquire b", "use ab", "release b ok", "release a ok"), log)
    }

    @Test
    fun `a failing build releases what was already acquired and skips the effect`() = runTest {
        val broken: Layer<Any?, String, String> = Layer.effect(Effect.fail("no config"))
        val app = resource("a").zipWith(broken) { a, b -> a + b }
        assertEquals(Exit.Failure(Cause.Fail("no config")), use.provide(app).runExit())
        assertEquals(listOf("acquire a", "release a failed"), log)
    }

    @Test
    fun `the effect's failure reaches the layer's release`() = runTest {
        val failing: Effect<String, String, String> = Effect.fail("boom")
        assertEquals(Exit.Failure(Cause.Fail("boom")), failing.provide(resource("a")).runExit())
        assertEquals(listOf("acquire a", "release a failed"), log)
    }

    @Test
    fun `an effect that needs nothing can still be provided a layer`() = runTest {
        // R = Any? accepts a Layer as a plain environment value too; the layer overload must win.
        val eff: UIO<Int> = Effect.succeed(1)
        assertEquals(1, eff.provide(resource("a")).run())
        assertEquals(listOf("acquire a", "release a ok"), log)
    }

    @Test
    fun `a shared layer whose build died fails every reference with the same defect`() = runTest {
        val shared: Layer<Any?, Nothing, String> = Layer.effect(Effect.die(boom))
        // The first reference recovers, so the second one gets the memoized failure.
        val first = Layer.effect(effect<Any?, Nothing, String> { shared.bind() }.catchAllCause { Effect.succeed("recovered") })
        val app = first.zipWith(Layer.gen<Any?, Nothing, String> { shared.bind() }) { a, b -> a + b }
        assertEquals(Exit.Failure(Cause.Die(boom)), use.provide(app).runExit())
    }

    @Test
    fun `orElse falls back when the build fails`() = runTest {
        val broken: Layer<Any?, String, String> = Layer.effect(Effect.fail("down"))
        assertEquals("backup", use.provide(broken.orElse(Layer.succeed("backup"))).run())
    }

    @Test
    fun `layer resources are released when the effect is interrupted`() = runTest {
        val exit = effect<Any?, Nothing, Exit<Nothing, Nothing>> {
            val fiber = use.andThen(Effect.never).provide(resource("a")).fork().bind()
            Effect.yieldNow.bind()
            fiber.interrupt.bind()
        }.run()
        assertEquals(Exit.Failure(Cause.Interrupt), exit)
        assertEquals(listOf("acquire a", "use a", "release a failed"), log)
    }

    @Test
    fun `acquireRelease releases on interruption with the Interrupt exit`() = runTest {
        var released: Exit<*, *>? = null
        val held = Effect.scoped(
            Effect.acquireReleaseExit(Effect.succeed("r")) { _, exit -> Effect.sync { released = exit } }
                .andThen(Effect.never),
        )
        effect<Any?, Nothing, Unit> {
            val fiber = held.fork().bind()
            Effect.yieldNow.bind()
            fiber.interrupt.bind()
        }.run()
        assertEquals(Exit.Failure(Cause.Interrupt), released)
    }
}
