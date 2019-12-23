package com.okue.demo.support

import brave.Tracing
import mu.KotlinLogging
import org.reactivestreams.Publisher
import org.springframework.context.annotation.Configuration
import reactor.core.CoreSubscriber
import reactor.core.Fuseable
import reactor.core.publisher.Hooks
import reactor.core.publisher.Operators
import java.util.function.Function
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Configuration
class ReactorConfiguration(
    private val tracing: Tracing
) {
    companion object {
        private val logger = KotlinLogging.logger {}
        val REACTOR_KEY: String = ReactorConfiguration::class.java.name
    }

    @PostConstruct
    fun setupHooks() {
        logger.trace("[ReactorConfiguration] Setup hooks")
        Hooks.onEachOperator(REACTOR_KEY, transformToMySubscriber())
    }

    @PreDestroy
    fun cleanupHooks() {
        logger.trace("[RequestConfiguration] Cleanup hooks")
        Hooks.resetOnEachOperator(REACTOR_KEY)
    }

    private fun <T> transformToMySubscriber(): Function<in Publisher<T>, out Publisher<T>> {
        return Operators.liftPublisher { publisher, coreSubscriber ->
            // if Flux/Mono #just, #empty or #error, there is no need to transform.
            if (publisher is Fuseable.ScalarCallable<*>) {
                coreSubscriber
            } else {
                logger.trace(
                    "Transform to MDCAwareSubscriber. coreSubscriber={}, ReactorContext={}",
                    coreSubscriber.javaClass.name, coreSubscriber.currentContext()
                )
                mySubscriber(coreSubscriber)
            }
        }
    }

    private fun <T> mySubscriber(coreSubscriber: CoreSubscriber<in T>): CoreSubscriber<in T> {
        val context = coreSubscriber.currentContext()
        val span = tracing.tracer().currentSpan()
        return BraveTracingAwareSubscriber(coreSubscriber, context, tracing, span)
    }
}
