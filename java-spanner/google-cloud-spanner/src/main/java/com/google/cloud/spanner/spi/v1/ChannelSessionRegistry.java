/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.spi.v1;

import com.google.cloud.spanner.spi.v1.SpannerRpc.ChannelPrimeSessionSource;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Thread-safe registry of live {@link ChannelPrimeSessionSource} instances shared by channel
 * priming ({@link DynamicChannelPoolPrimer}) and DirectPath recovery probing ({@link
 * DirectPathFallbackProber}).
 *
 * <p>Each live multiplexed-session database client registers itself on creation and unregisters
 * itself when invalidated or closed. Consecutive session lookups rotate across registered sources
 * and skip sources whose session is not yet ready, without blocking.
 */
final class ChannelSessionRegistry {
  private final CopyOnWriteArrayList<ChannelPrimeSessionSource> sources =
      new CopyOnWriteArrayList<>();
  private final AtomicInteger nextSourceIndex = new AtomicInteger();

  /** Registers a session source by reference identity. Repeated registration is a no-op. */
  void register(ChannelPrimeSessionSource source) {
    Preconditions.checkNotNull(source);
    synchronized (sources) {
      for (ChannelPrimeSessionSource existing : sources) {
        if (existing == source) {
          return;
        }
      }
      sources.add(source);
    }
  }

  /** Deregisters a session source. Repeated deregistration is a no-op. */
  void unregister(ChannelPrimeSessionSource source) {
    Preconditions.checkNotNull(source);
    synchronized (sources) {
      for (int i = 0; i < sources.size(); i++) {
        if (sources.get(i) == source) {
          sources.remove(i);
          return;
        }
      }
    }
  }

  /** Returns an immutable snapshot of registered sources in registration order. */
  List<ChannelPrimeSessionSource> getSources() {
    return ImmutableList.copyOf(sources);
  }

  /** Returns a currently available session name without blocking, rotating the starting source. */
  @Nullable
  String nextSessionName() {
    List<ChannelPrimeSessionSource> snapshot = ImmutableList.copyOf(sources);
    int size = snapshot.size();
    if (size == 0) {
      return null;
    }
    int start = Math.floorMod(nextSourceIndex.getAndIncrement(), size);
    for (int offset = 0; offset < size; offset++) {
      ChannelPrimeSessionSource source = snapshot.get((start + offset) % size);
      String sessionName = source.getChannelPrimeSessionName();
      if (sessionName != null) {
        return sessionName;
      }
    }
    return null;
  }
}
