package keffect

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.ConcurrentHashMap

/**
 * `Layer<ROut, E, RIn>` — a recipe for building services of type [ROut] from [RIn],
 * possibly failing with [E] and possibly acquiring resources.
 *
 * Like in Effect-TS, layers are **memoized**: within one `provide(layer)`, a layer value
 * that is referenced several times in the dependency graph is built only once, and
 * resources it acquires live as long as the effect it was provided to.
 */
class Layer<in RIn, out E, out ROut> @PublishedApi internal constructor(
    private val builder: Effect<RIn, E, ROut>,
    val name: String? = null,
) {
    /**
     * The build of this layer, as an effect: memoized within the enclosing `provide(layer)`,
     * so a layer referenced several times in one graph is built once.
     */
    internal fun build(): Effect<RIn, E, ROut> = Effect { env ->
        val memo = memoMaps.current() ?: return@Effect builder.unsafeRun(env)
        val fresh = CompletableDeferred<Exit<*, *>>()
        val existing = memo.builds.putIfAbsent(this, fresh)
        if (existing != null) {
            @Suppress("UNCHECKED_CAST")
            return@Effect existing.await().getOrRaise() as ROut
        }
        // Shared as a value: awaiting a failed Deferred would rethrow a copy of a defect.
        val exit = try {
            builder.runToExit(env)
        } catch (c: CancellationException) {
            fresh.completeExceptionally(c)
            throw c
        }
        fresh.complete(exit)
        exit.getOrRaise()
    }

    override fun toString() = "Layer(${name ?: "anonymous"})"

    companion object {
        /** `Layer.succeed` — a layer from a ready value. */
        fun <A> succeed(value: A): Layer<Any?, Nothing, A> = Layer(Effect.succeed(value))

        /** `Layer.effect` — a layer built by an effect. Use `acquireRelease` inside for resources (`Layer.scoped`). */
        fun <RIn, E, ROut> effect(build: Effect<RIn, E, ROut>, name: String? = null): Layer<RIn, E, ROut> =
            Layer(build, name)

        /** Builder form: `Layer.gen<Config, DbError, Db> { val cfg = env; ... }` */
        fun <RIn, E, ROut> gen(name: String? = null, block: suspend EffectScope<RIn, E>.() -> ROut): Layer<RIn, E, ROut> =
            Layer(effect(block), name)
    }
}

private val memoMaps = ContextSlot<MemoMap>()

private class MemoMap {
    val builds = ConcurrentHashMap<Layer<*, *, *>, CompletableDeferred<Exit<*, *>>>()
}

// ---- Composition ---------------------------------------------------------------

/** `Layer.map` */
fun <RIn, E, A, B> Layer<RIn, E, A>.map(f: (A) -> B): Layer<RIn, E, B> = Layer.effect(build().map(f), name)

/** `Layer.provide`: feed this layer's output into [next]. */
fun <RIn, E, A, B> Layer<RIn, E, A>.andThen(next: Layer<A, E, B>): Layer<RIn, E, B> =
    Layer.effect(Effect { env -> next.build().unsafeRun(build().unsafeRun(env)) })

/** `Layer.merge` — build both (from the same input) and combine their outputs. */
fun <RIn, E, A, B, C> Layer<RIn, E, A>.zipWith(that: Layer<RIn, E, B>, f: (A, B) -> C): Layer<RIn, E, C> =
    Layer.effect(build().zipWith(that.build(), f))

/** Like [zipWith] but builds both sides concurrently. */
fun <RIn, E, A, B, C> Layer<RIn, E, A>.zipWithPar(that: Layer<RIn, E, B>, f: (A, B) -> C): Layer<RIn, E, C> =
    Layer.effect(build().zipWithPar(that.build(), f))

/** `Layer.catchAll` — fall back to another layer when building fails. */
fun <RIn, E, E2, A> Layer<RIn, E, A>.orElse(that: Layer<RIn, E2, A>): Layer<RIn, E2, A> =
    Layer.effect(build().catchAll { that.build() })

// ---- Providing -------------------------------------------------------------------

/**
 * `Effect.provide(layer)` — build the layer, run the effect with its output, then
 * release everything the layer acquired.
 */
fun <RIn, E, R, A> Effect<R, E, A>.provide(layer: Layer<RIn, E, R>): Effect<RIn, E, A> = Effect { env ->
    // A fresh memo map per provide: builds are shared within this layer graph,
    // and never outlive the scope that owns their resources.
    withScope {
        val services = memoMaps.with(MemoMap()) { layer.build().unsafeRun(env) }
        unsafeRun(services)
    }
}

/**
 * The same as `provide(layer)` above, for effects that need nothing (`R = Any?`). For those,
 * `provide(env: R)` accepts a Layer as a plain value too, and without this narrower receiver
 * the call would be ambiguous.
 */
@JvmName("provideToIndependent")
fun <RIn, E, A> Effect<Any?, E, A>.provide(layer: Layer<RIn, E, Any?>): Effect<RIn, E, A> =
    provide<RIn, E, Any?, A>(layer) // explicit type arguments select the general overload
