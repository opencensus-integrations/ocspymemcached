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

import io.opencensus.common.Scope;
import io.orijtech.integrations.ocspymemcached.Observability.TrackingOperation;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;

public class OcWrapClient extends MemcachedClient {

  public OcWrapClient(InetSocketAddress... ia) throws IOException {
    super(ia);
  }

  public OcWrapClient(List<InetSocketAddress> addrs) throws IOException {
    super(addrs);
  }

  public OcWrapClient(ConnectionFactory cf, List<InetSocketAddress> addrs) throws IOException {
    super(cf, addrs);
  }

  @Override
  public OperationFuture<Boolean> add(String key, int exp, Object o) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.add", key);

    final OperationFuture<Boolean> res =
        super.add(key, exp, o)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<Boolean> add(String key, int exp, T o, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.add", key);

    final OperationFuture<Boolean> res =
        super.add(key, exp, o, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public boolean addObserver(ConnectionObserver obs) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.addObserver");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.addObserver(obs);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public OperationFuture<Boolean> append(long cas, String key, Object val) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.append", key);

    final OperationFuture<Boolean> res =
        super.append(cas, key, val)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<Boolean> append(long cas, String key, T val, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.append", key);

    final OperationFuture<Boolean> res =
        super.append(cas, key, val, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(
      String key, long casId, int exp, T value, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncCAS", key);

    final OperationFuture<CASResponse> res =
        super.asyncCAS(key, casId, exp, value, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, Object value) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncCAS", key);

    final OperationFuture<CASResponse> res =
        super.asyncCAS(key, casId, value)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(
      String key, long casId, T value, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncCAS", key);

    final OperationFuture<CASResponse> res =
        super.asyncCAS(key, casId, value, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<Long> asyncDecr(String key, int by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncDecr", key);

    final OperationFuture<Long> res =
        super.asyncDecr(key, by)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public GetFuture<Object> asyncGet(String key) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncGet", key);

    final GetFuture<Object> res =
        super.asyncGet(key)
            .addListener(Observability.createGetFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> GetFuture<T> asyncGet(String key, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncGet", key);

    final GetFuture<T> res =
        super.asyncGet(key, tc)
            .addListener(Observability.createGetFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.asyncGetBulk");

    final BulkFuture<Map<String, Object>> res = super.asyncGetBulk(keys);
    res.addListener(Observability.createBulkGetFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.asyncGetBulk");

    final BulkFuture<Map<String, T>> res = super.asyncGetBulk(keys, tc);
    res.addListener(Observability.createBulkGetFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncGetBulk", keys);

    final BulkFuture<Map<String, Object>> res = super.asyncGetBulk(keys);
    res.addListener(Observability.createBulkGetFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc, String... keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncGetBulk", keys);

    final BulkFuture<Map<String, T>> res = super.asyncGetBulk(tc, keys);
    res.addListener(Observability.createBulkGetFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public OperationFuture<CASValue<Object>> asyncGets(String key) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncGets", key);

    final OperationFuture<CASValue<Object>> res =
        super.asyncGets(key)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public OperationFuture<Long> asyncIncr(String key, int by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.asyncIncr", key);

    final OperationFuture<Long> res =
        super.asyncIncr(key, by)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));

    return res;
  }

  @Override
  public CASResponse cas(String key, long casId, Object value) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.cas", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.cas(key, casId, value);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public CASResponse cas(String key, long casId, int exp, Object value) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.cas", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.cas(key, casId, exp, value);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public <T> CASResponse cas(String key, long casId, int exp, T value, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.cas", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.cas(key, casId, exp, value, tc);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public <T> CASResponse cas(String key, long casId, T value, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.cas", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.cas(key, casId, value, tc);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public CASValue<Object> getAndTouch(String key, int exp) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.getAndTouch", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getAndTouch(key, exp);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public CASValue<Object> gets(String key) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.gets", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.gets(key);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Object get(String key) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.get", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.get(key);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public <T> T get(String key, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.get", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.get(key, tc);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<String, Object> getBulk(Iterator<String> keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.getBulk");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getBulk(keys);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<String, Object> getBulk(Collection<String> keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.getBulk");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getBulk(keys);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.getBulk", keys);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getBulk(tc, keys);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<String, Object> getBulk(String... keys) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.getBulk", keys);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getBulk(keys);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.gets", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.gets(key, tc);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats(String prefix) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.getStats", prefix);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getStats(prefix);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<SocketAddress, String> getVersions() {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.getVersions");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getVersions();
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long incr(String key, long by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.incr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.incr(key, by);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long incr(String key, int by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.incr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.incr(key, by);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long incr(String key, int by, long def) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.incr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.incr(key, by, def);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long incr(String key, int by, long def, int exp) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.incr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.incr(key, by, def, exp);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long decr(String key, long by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.decr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.decr(key, by);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long decr(String key, int by) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.decr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.decr(key, by);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long incr(String key, long by, long def, int exp) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.incr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.incr(key, by, def, exp);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public OperationFuture<Boolean> prepend(long cas, String key, Object val) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.prepend", key);

    final OperationFuture<Boolean> res =
        super.prepend(cas, key, val)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<Boolean> prepend(long cas, String key, T val, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.prepend", key);

    final OperationFuture<Boolean> res =
        super.prepend(cas, key, val, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public long decr(String key, long by, long def, int exp) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.decr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.decr(key, by, def, exp);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public long decr(String key, int by, long def, int exp) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.decr", key);

    try (Scope ws = trackingOperation.withSpan()) {
      return super.decr(key, by, def, exp);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public OperationFuture<Boolean> delete(String key) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.delete", key);

    final OperationFuture<Boolean> res =
        super.delete(key)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<Boolean> flush() {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.flush");

    final OperationFuture<Boolean> res =
        super.flush()
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<Boolean> flush(int delay) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.flush");

    final OperationFuture<Boolean> res =
        super.flush(delay)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<Boolean> replace(String key, int exp, Object o) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.replace");

    final OperationFuture<Boolean> res =
        super.replace(key, exp, o)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<Boolean> replace(String key, int exp, T o, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.replace");

    final OperationFuture<Boolean> res =
        super.replace(key, exp, o, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public OperationFuture<Boolean> set(String key, int exp, Object o) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.set");

    final OperationFuture<Boolean> res =
        super.set(key, exp, o)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public <T> OperationFuture<Boolean> set(String key, int exp, T o, Transcoder<T> tc) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.set");

    final OperationFuture<Boolean> res =
        super.set(key, exp, o, tc)
            .addListener(Observability.createOperationFutureCompletionListener(trackingOperation));
    return res;
  }

  @Override
  public Set<String> listSaslMechanisms() {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan(
            "net.spy.memcached.MemcachedClient.listSaslMechanisms");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.listSaslMechanisms();
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats() {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.getStats");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.getStats();
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public void shutdown() {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.shutdown");

    try (Scope ws = trackingOperation.withSpan()) {
      super.shutdown();
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    TrackingOperation trackingOperation =
        Observability.createRoundtripTrackingSpan("net.spy.memcached.MemcachedClient.shutdown");

    try (Scope ws = trackingOperation.withSpan()) {
      return super.shutdown(timeout, unit);
    } catch (Exception e) {
      trackingOperation.recordException(e);
      throw e;
    } finally {
      trackingOperation.end();
    }
  }
}
