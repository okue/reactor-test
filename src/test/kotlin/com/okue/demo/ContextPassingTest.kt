package com.okue.demo

import brave.Tracer
import brave.Tracing
import com.okue.demo.support.ReactorConfiguration
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toFlux
import zipkin2.reporter.Reporter
import java.util.concurrent.Executors

class ContextPassingTest {
    companion object {
        private val log = KotlinLogging.logger { }
    }

    private val tracing = Tracing.newBuilder().spanReporter(Reporter.CONSOLE).build()
    private val reactorConfiguration = ReactorConfiguration(tracing)

    @BeforeEach
    fun setup() {
//        reactorConfiguration.setupHooks()
    }

    @AfterEach
    fun terminate() {
//        reactorConfiguration.cleanupHooks()
    }

    private fun Tracer.createSpan(spanName: String) {
        val childSpan = this.startScopedSpan(spanName)
        Thread.sleep(10)
        log.info("finish $spanName")
        childSpan.finish()
    }

    @Test
    fun test1() {
//        val parent = tracing.currentTraceContext()?.get()
        val tracer = tracing.tracer()
//        val parentSpan = tracer.startScopedSpanWithParent("parent", parent)
        val parentSpan = tracer.startScopedSpan("parent")

        val simpleMono = { i: Int ->
            Mono.just(i).map {
                Tracing.currentTracer().createSpan("$it-span-mono")
                return@map it
            }
        }

        (1..3).toFlux()
            .parallel()
            .runOn(Schedulers.newParallel("my-para", 3))
            .flatMap {
                Tracing.currentTracer().createSpan("$it-span")
                return@flatMap simpleMono(it)
            }.subscribe()
        Thread.sleep(100)
        log.info("finish parent-span")
        parentSpan.finish()
    }

    private val myDispatcher = Executors.newFixedThreadPool(1).asCoroutineDispatcher()

    @Test
    fun test2() = runBlocking {
        val parent = tracing.currentTraceContext()?.get()
        val tracer = tracing.tracer()
        val parentSpan = tracer.startScopedSpanWithParent("parent", parent)
        MDC.put("xxx", "xxx")

        val executeJob = {
            val job = launch(myDispatcher) {
                log.info("Hey -> ${MDC.get("xxx")}")
                delay(100)
                Tracing.currentTracer().createSpan("Uho")
            }
            job
        }
        val jobs = (1..10).map {
            executeJob()
        }
        jobs.forEach { it.join() }

//        val a = mono {
//            log.info("Hey")
//            tracer.createSpan("Hey")
//        }
//        a.block()
        log.info("finish parentSpan -> ${MDC.get("xxx")}")
        parentSpan.finish()
    }
}
