// Copyright 2019, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.orijtech.integrations.ocspymemcached;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagContextBuilder;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.orijtech.integrations.ocspymemcached.Observability.OperationFutureCompletionListener;
import io.orijtech.integrations.ocspymemcached.Observability.TrackingOperation;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.NoopOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ObservabilityTest {
  @Mock private ViewManager mockViewManager;
  @Mock private Tagger mockTagger;
  @Mock private StatsRecorder mockStatsRecorder;
  @Mock private Tracer mockTracer;
  @Mock private MeasureMap mockMeasureMap;
  @Mock private TagContextBuilder mockTagContextBuilder;
  @Mock private TagContext mockTagContext;
  @Mock private Span mockSpan;
  @Mock private SpanBuilder mockSpanBuilder;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Mockito.doNothing().when(mockViewManager).registerView(any(View.class));
    Mockito.when(mockTagger.currentBuilder()).thenReturn(mockTagContextBuilder);
    Mockito.when(mockTagContextBuilder.put(any(TagKey.class), any(TagValue.class)))
        .thenReturn(mockTagContextBuilder);
    Mockito.when(mockTagContextBuilder.build()).thenReturn(mockTagContext);
    Mockito.when(mockStatsRecorder.newMeasureMap()).thenReturn(mockMeasureMap);
    Mockito.when(mockMeasureMap.put(any(MeasureDouble.class), anyDouble()))
        .thenReturn(mockMeasureMap);
    Mockito.doNothing().when(mockMeasureMap).record(any(TagContext.class));
    Mockito.when(mockTracer.spanBuilderWithExplicitParent(anyString(), anyObject()))
        .thenReturn(mockSpanBuilder);
    Mockito.when(mockSpanBuilder.startSpan()).thenReturn(mockSpan);
    Mockito.doNothing().when(mockSpan).putAttribute(anyString(), any(AttributeValue.class));
  }

  @Test
  public void trackingOperation_endToEnd() {
    TrackingOperation trackingOperation =
        new TrackingOperation(
            "net.spy.memcached.MemcachedClient.prepend",
            mockStatsRecorder,
            mockTagger,
            mockTracer,
            "keyA",
            "keyB");
    trackingOperation.end();
    Mockito.verify(mockTagger, Mockito.times(1)).currentBuilder();
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(
            eq(Observability.METHOD),
            eq(TagValue.create("net.spy.memcached.MemcachedClient.prepend")));
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(eq(Observability.STATUS), eq(Observability.VALUE_OK));
    Mockito.verify(mockStatsRecorder, Mockito.times(1)).newMeasureMap();
    Mockito.verify(mockMeasureMap, Mockito.times(1))
        .put(eq(Observability.MEASURE_LATENCY_MS), anyDouble());
    Mockito.verify(mockMeasureMap, Mockito.times(2))
        .put(eq(Observability.MEASURE_LENGTH), anyLong());
    Mockito.verify(mockMeasureMap, Mockito.times(1)).record(any(TagContext.class));
    Mockito.verify(mockSpan, Mockito.times(1)).end();
  }

  @Test
  public void trackingOperation_end_recordException() {
    TrackingOperation trackingOperation =
        new TrackingOperation(
            "net.spy.memcached.MemcachedClient.asyncCAS",
            mockStatsRecorder,
            mockTagger,
            mockTracer,
            "key1",
            "key2");
    IOException exception = new IOException("closed connection");
    trackingOperation.recordException(exception);
    trackingOperation.end();
    Mockito.verify(mockTagger, Mockito.times(1)).currentBuilder();
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(
            eq(Observability.METHOD),
            eq(TagValue.create("net.spy.memcached.MemcachedClient.asyncCAS")));
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(eq(Observability.ERROR), eq(TagValue.create(exception.toString())));
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(eq(Observability.STATUS), eq(Observability.VALUE_ERROR));
    Mockito.verify(mockSpan, Mockito.times(1))
        .setStatus(eq(Status.UNKNOWN.withDescription(exception.toString())));
    Mockito.verify(mockStatsRecorder, Mockito.times(1)).newMeasureMap();
    Mockito.verify(mockMeasureMap, Mockito.times(1))
        .put(eq(Observability.MEASURE_LATENCY_MS), anyDouble());
    Mockito.verify(mockMeasureMap, Mockito.times(1)).record(any(TagContext.class));
    Mockito.verify(mockSpan, Mockito.times(1)).end();
  }

  @Test
  public void trackingOperation_withSpan() {
    TrackingOperation trackingOperation =
        new TrackingOperation(
            "net.spy.memcached.MemcachedClient.set",
            mockStatsRecorder,
            mockTagger,
            mockTracer,
            "key1",
            "key2");
    trackingOperation.withSpan();
    Mockito.verify(mockTracer, Mockito.times(1))
        .spanBuilderWithExplicitParent("net.spy.memcached.MemcachedClient.set", null);
    Mockito.verify(mockSpanBuilder, Mockito.times(1)).startSpan();
  }

  @Test
  public void trackingOperation_operationFutureListener() {
    TrackingOperation trackingOperation =
        new TrackingOperation(
            "net.spy.memcached.MemcachedClient.delete",
            mockStatsRecorder,
            mockTagger,
            mockTracer,
            "key1");

    Boolean bool = true;
    AtomicReference<Boolean> bref = new AtomicReference<Boolean>(bool);
    CountDownLatch latch = new CountDownLatch(1);
    ConnectionFactory cf = new DefaultConnectionFactory();
    OperationFuture<Boolean> future =
        new OperationFuture<Boolean>("foo", latch, bref, 10, cf.getListenerExecutorService());
    NoopOperation noop =
        cf.getOperationFactory()
            .noop(
                new OperationCallback() {
                  @Override
                  public void receivedStatus(OperationStatus status) {
                    future.set(status.isSuccess(), status);
                  }

                  @Override
                  public void complete() {
                    latch.countDown();
                  }
                });
    future.setOperation(noop);

    OperationFutureCompletionListener futureListener =
        Observability.createOperationFutureCompletionListener(trackingOperation);

    // Mimick asynchronous usage.
    try {
      Boolean result = future.get();
    } catch (Exception e) {
      System.err.println("future.get exception: " + e);
    }

    try {
      futureListener.onComplete(future);
    } catch (Exception e) {
      System.err.println("futureListener.onComplete exception: " + e);
    }

    // Now check the results, ensuring that end() was invoked asynchronously.
    // and that we have a span with the proper status and the various stats.
    Mockito.verify(mockTagger, Mockito.times(1)).currentBuilder();
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(
            eq(Observability.METHOD),
            eq(TagValue.create("net.spy.memcached.MemcachedClient.delete")));
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(eq(Observability.ERROR), eq(TagValue.create("timed out")));
    Mockito.verify(mockTagContextBuilder, Mockito.times(1))
        .put(eq(Observability.STATUS), eq(Observability.VALUE_ERROR));
    Mockito.verify(mockSpan, Mockito.times(1))
        .setStatus(eq(Status.UNKNOWN.withDescription("timed out")));
    Mockito.verify(mockStatsRecorder, Mockito.times(1)).newMeasureMap();
    Mockito.verify(mockMeasureMap, Mockito.times(1))
        .put(eq(Observability.MEASURE_LATENCY_MS), anyDouble());
    Mockito.verify(mockMeasureMap, Mockito.times(1)).record(any(TagContext.class));
    Mockito.verify(mockSpan, Mockito.times(1)).end();
  }

  @Test
  public void trackingOperation_operationFutureListener_nullKeysNoPanics() {
    TrackingOperation trackingOperation =
        new TrackingOperation("net.spy.memcached.MemcachedClient.foo-with-null-keys");
    trackingOperation.end();
  }
}
