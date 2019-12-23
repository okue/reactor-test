package com.okue.demo.support

import brave.Span
import brave.Tracing
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import reactor.core.CoreSubscriber
import reactor.util.context.Context

class BraveTracingAwareSubscriber<T>(
    private val subscriber: Subscriber<in T>,
    private val context: Context,
    tracing: Tracing,
    root: Span?
) : CoreSubscriber<T> {

    private val currentTraceContext = tracing.currentTraceContext()
    private val traceContext = root?.context()

    override fun onSubscribe(subscription: Subscription) {
        currentTraceContext.maybeScope(traceContext).use {
            subscriber.onSubscribe(subscription)
        }
    }

    override fun onNext(o: T) {
        currentTraceContext.maybeScope(traceContext).use {
            subscriber.onNext(o)
        }
    }

    override fun onError(throwable: Throwable) {
        currentTraceContext.maybeScope(traceContext).use {
            subscriber.onError(throwable)
        }
    }

    override fun onComplete() {
        currentTraceContext.maybeScope(traceContext).use {
            subscriber.onComplete()
        }
    }

    override fun currentContext(): Context {
        return context
    }
}
