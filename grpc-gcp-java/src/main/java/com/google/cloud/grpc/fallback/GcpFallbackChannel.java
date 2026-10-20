/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.grpc.fallback;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import javax.annotation.Nullable;

public class GcpFallbackChannel extends ManagedChannel {
  private static final Logger logger = Logger.getLogger(GcpFallbackChannel.class.getName());
  static final String INIT_FAILURE_REASON = "init failure";
  private final GcpFallbackChannelOptions options;
  // Primary channel that was provided in constructor.
  @Nullable private final ManagedChannel primaryDelegateChannel;
  // Fallback channel that was provided in constructor.
  @Nullable private final ManagedChannel fallbackDelegateChannel;
  // Wrapped primary channel to be used for RPCs.
  private final Channel primaryChannel;
  // Wrapped fallback channel to be used for RPCs.
  private final Channel fallbackChannel;
  private final GcpFallbackState fallbackState;
  private final boolean ownsFallbackState;
  private final GcpFallbackOpenTelemetry openTelemetry;

  private final AtomicBoolean channelInFallback = new AtomicBoolean(false);
  private final AtomicLong primaryProbeSuccesses = new AtomicLong(0);
  private final AtomicLong firstPrimaryProbeSuccessNanos = new AtomicLong(0);
  private final Set<Runnable> stateChangeCallbacks = ConcurrentHashMap.newKeySet();
  private boolean isShutdown = false;

  private final ScheduledExecutorService execService;
  private volatile ScheduledFuture<?> primaryProbeFuture = null;
  private volatile ScheduledFuture<?> fallbackProbeFuture = null;

  public GcpFallbackChannel(
      GcpFallbackChannelOptions options,
      ManagedChannel primaryChannel,
      ManagedChannel fallbackChannel) {
    this(options, primaryChannel, fallbackChannel, null);
  }

  public GcpFallbackChannel(
      GcpFallbackChannelOptions options,
      ManagedChannelBuilder<?> primaryChannelBuilder,
      ManagedChannelBuilder<?> fallbackChannelBuilder) {
    this(options, primaryChannelBuilder, fallbackChannelBuilder, null);
  }

  @VisibleForTesting
  GcpFallbackChannel(
      GcpFallbackChannelOptions options,
      ManagedChannelBuilder<?> primaryChannelBuilder,
      ManagedChannelBuilder<?> fallbackChannelBuilder,
      ScheduledExecutorService execService) {
    checkNotNull(options);
    checkNotNull(primaryChannelBuilder);
    checkNotNull(fallbackChannelBuilder);
    this.options = options;
    if (options.getSharedState() != null) {
      this.fallbackState = options.getSharedState();
      this.ownsFallbackState = false;
    } else {
      this.fallbackState =
          execService != null ? new GcpFallbackState(execService) : new GcpFallbackState();
      this.ownsFallbackState = true;
    }
    if (options.getGcpOpenTelemetry() != null) {
      this.openTelemetry = options.getGcpOpenTelemetry();
    } else {
      this.openTelemetry = GcpFallbackOpenTelemetry.newBuilder().build();
    }
    ManagedChannel primaryChannel = null;
    try {
      primaryChannel = primaryChannelBuilder.build();
    } catch (Exception e) {
      logger.warning(
          String.format(
              "Primary channel initialization failed: %s. Will use fallback channel.",
              e.getMessage()));
    }
    primaryDelegateChannel = primaryChannel;

    ManagedChannel fallbackChannel = null;
    try {
      fallbackChannel = fallbackChannelBuilder.build();
    } catch (Exception e) {
      if (primaryChannel == null) {
        throw new RuntimeException(
            "Both primary and fallback channels initialization failed: " + e.getMessage(), e);
      }

      logger.warning(
          String.format(
              "Fallback channel initialization failed: %s. Will use only the primary channel.",
              e.getMessage()));
    }
    fallbackDelegateChannel = fallbackChannel;

    if (primaryDelegateChannel != null) {
      this.primaryChannel =
          ClientInterceptors.intercept(
              primaryDelegateChannel, new MonitoringInterceptor(this::processPrimaryStatusCode));
    } else {
      this.primaryChannel = null;
    }

    if (fallbackDelegateChannel != null) {
      this.fallbackChannel =
          ClientInterceptors.intercept(
              fallbackDelegateChannel, new MonitoringInterceptor(this::processFallbackStatusCode));
    } else {
      this.fallbackChannel = null;
    }

    this.execService = fallbackState.getOrCreateExecutorService(options, execService);
    init();
  }

  @VisibleForTesting
  GcpFallbackChannel(
      GcpFallbackChannelOptions options,
      ManagedChannel primaryChannel,
      ManagedChannel fallbackChannel,
      ScheduledExecutorService execService) {
    checkNotNull(options);
    checkNotNull(primaryChannel);
    checkNotNull(fallbackChannel);
    this.options = options;
    if (options.getSharedState() != null) {
      this.fallbackState = options.getSharedState();
      this.ownsFallbackState = false;
    } else {
      this.fallbackState =
          execService != null ? new GcpFallbackState(execService) : new GcpFallbackState();
      this.ownsFallbackState = true;
    }
    this.execService = fallbackState.getOrCreateExecutorService(options, execService);
    if (options.getGcpOpenTelemetry() != null) {
      this.openTelemetry = options.getGcpOpenTelemetry();
    } else {
      this.openTelemetry = GcpFallbackOpenTelemetry.newBuilder().build();
    }
    primaryDelegateChannel = primaryChannel;
    fallbackDelegateChannel = fallbackChannel;
    ClientInterceptor primaryMonitorInterceptor =
        new MonitoringInterceptor(this::processPrimaryStatusCode);
    this.primaryChannel =
        ClientInterceptors.intercept(primaryDelegateChannel, primaryMonitorInterceptor);
    ClientInterceptor fallbackMonitorInterceptor =
        new MonitoringInterceptor(this::processFallbackStatusCode);
    this.fallbackChannel =
        ClientInterceptors.intercept(fallbackDelegateChannel, fallbackMonitorInterceptor);
    init();
  }

  public boolean isInFallbackMode() {
    return (channelInFallback.get() && fallbackChannel != null) || primaryChannel == null;
  }

  @VisibleForTesting
  GcpFallbackState getFallbackState() {
    return fallbackState;
  }

  @VisibleForTesting
  AtomicLong getPrimaryProbeSuccesses() {
    return primaryProbeSuccesses;
  }

  private synchronized void registerStateChangeCallback(Runnable callback) {
    if (!isShutdown) {
      stateChangeCallbacks.add(callback);
    }
  }

  private synchronized void unregisterStateChangeCallback(Runnable callback) {
    stateChangeCallbacks.remove(callback);
  }

  private List<Runnable> snapshotAndClearCallbacks() {
    List<Runnable> callbacks = new ArrayList<>(stateChangeCallbacks);
    stateChangeCallbacks.clear();
    return callbacks;
  }

  private void runStateChangeCallbacks(List<Runnable> callbacks) {
    if (callbacks == null) {
      return;
    }
    for (Runnable callback : callbacks) {
      try {
        callback.run();
      } catch (Throwable t) {
      }
    }
  }

  boolean enterFallbackMode() {
    if (fallbackChannel == null) {
      return false;
    }
    List<Runnable> callbacks = null;
    synchronized (this) {
      if (isShutdown || !channelInFallback.compareAndSet(false, true)) {
        return false;
      }
      primaryProbeSuccesses.set(0);
      firstPrimaryProbeSuccessNanos.set(0);
      callbacks = snapshotAndClearCallbacks();
    }
    runStateChangeCallbacks(callbacks);
    return true;
  }

  /**
   * Records a primary probe result on this channel and exits fallback mode if consecutive probe
   * success count and duration criteria are met.
   */
  @VisibleForTesting
  void recordPrimaryProbeResult(boolean success) {
    if (!options.isEnableRecovery() || fallbackChannel == null || primaryDelegateChannel == null) {
      return;
    }
    boolean recovered = false;
    List<Runnable> callbacks = null;
    synchronized (this) {
      if (isShutdown || !channelInFallback.get()) {
        return;
      }
      if (!success) {
        primaryProbeSuccesses.set(0);
        firstPrimaryProbeSuccessNanos.set(0);
        return;
      }
      long nowNanos = System.nanoTime();
      long firstSuccessNanos =
          firstPrimaryProbeSuccessNanos.updateAndGet(prev -> prev == 0 ? nowNanos : prev);
      long primaryProbeSuccessCount = primaryProbeSuccesses.incrementAndGet();

      boolean durationSatisfied = true;
      if (options.getMinPrimaryProbeSuccessDuration() != null
          && !options.getMinPrimaryProbeSuccessDuration().isZero()
          && !options.getMinPrimaryProbeSuccessDuration().isNegative()) {
        long elapsedNanos = nowNanos - firstSuccessNanos;
        durationSatisfied = elapsedNanos >= options.getMinPrimaryProbeSuccessDuration().toNanos();
      }

      if (primaryProbeSuccessCount >= options.getMinPrimaryProbeSuccessCount()
          && durationSatisfied) {
        primaryProbeSuccesses.set(0);
        firstPrimaryProbeSuccessNanos.set(0);
        channelInFallback.set(false);
        callbacks = snapshotAndClearCallbacks();
        recovered = true;
      }
    }
    if (recovered) {
      fallbackState.onChannelRecovered();
      openTelemetry
          .getModule()
          .reportFallback(options.getFallbackChannelName(), options.getPrimaryChannelName());
      runStateChangeCallbacks(callbacks);
    }
  }

  private void init() {
    if (this.fallbackChannel != null && this.primaryChannel != null) {
      fallbackState.setFallbackAvailable(true);
    }
    fallbackState.registerChannel(this);
    if (options.getPrimaryProbingFunction() != null) {
      this.primaryProbeFuture =
          fallbackState.scheduleTask(
              this::probePrimary,
              options.getPrimaryProbingInterval().toMillis(),
              options.getPrimaryProbingInterval().toMillis(),
              MILLISECONDS);
    }

    if (options.getFallbackProbingFunction() != null) {
      this.fallbackProbeFuture =
          fallbackState.scheduleTask(
              this::probeFallback,
              options.getFallbackProbingInterval().toMillis(),
              options.getFallbackProbingInterval().toMillis(),
              MILLISECONDS);
    }

    fallbackState.startPeriodicEvaluation(options);
  }

  private void processPrimaryStatusCode(Status.Code statusCode) {
    if (!isInFallbackMode()) {
      if (options.getErroneousStates().contains(statusCode)) {
        fallbackState.getPrimaryFailures().increment();
      } else {
        fallbackState.getPrimarySuccesses().increment();
      }
    }
    openTelemetry.getModule().reportStatus(options.getPrimaryChannelName(), statusCode);
  }

  private void processFallbackStatusCode(Status.Code statusCode) {
    if (options.getErroneousStates().contains(statusCode)) {
      fallbackState.getFallbackFailures().increment();
    } else {
      fallbackState.getFallbackSuccesses().increment();
    }
    openTelemetry.getModule().reportStatus(options.getFallbackChannelName(), statusCode);
  }

  private void probePrimary() {
    if (!isInFallbackMode()) {
      return;
    }
    String result = "";
    if (primaryDelegateChannel == null) {
      result = INIT_FAILURE_REASON;
    } else {
      try {
        result = options.getPrimaryProbingFunction().apply(primaryDelegateChannel);
      } catch (Exception e) {
        result = e.getClass().getSimpleName();
      }
    }
    // Report metric based on result.
    openTelemetry.getModule().reportProbeResult(options.getPrimaryChannelName(), result);
    if (fallbackChannel != null && primaryDelegateChannel != null) {
      recordPrimaryProbeResult("".equals(result));
    }
  }

  private void probeFallback() {
    String result = "";
    if (fallbackDelegateChannel == null) {
      result = INIT_FAILURE_REASON;
    } else {
      try {
        result = options.getFallbackProbingFunction().apply(fallbackDelegateChannel);
      } catch (Exception e) {
        result = e.getClass().getSimpleName();
      }
    }
    // Report metric based on result.
    openTelemetry.getModule().reportProbeResult(options.getFallbackChannelName(), result);
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    if (isInFallbackMode()) {
      return fallbackChannel.newCall(methodDescriptor, callOptions);
    }

    return primaryChannel.newCall(methodDescriptor, callOptions);
  }

  @Override
  public String authority() {
    if (isInFallbackMode()) {
      return fallbackChannel.authority();
    }

    return primaryChannel.authority();
  }

  @Override
  public ConnectivityState getState(boolean requestConnection) {
    if (isInFallbackMode()) {
      if (fallbackDelegateChannel != null) {
        return fallbackDelegateChannel.getState(requestConnection);
      }
      return ConnectivityState.SHUTDOWN;
    }

    if (primaryDelegateChannel != null) {
      return primaryDelegateChannel.getState(requestConnection);
    }
    return ConnectivityState.SHUTDOWN;
  }

  @Override
  public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
    boolean initialFallbackMode = isInFallbackMode();
    ManagedChannel delegate =
        initialFallbackMode ? fallbackDelegateChannel : primaryDelegateChannel;
    AtomicReference<Runnable> callbackRef = new AtomicReference<>(callback);
    Runnable once =
        () -> {
          Runnable cb = callbackRef.getAndSet(null);
          if (cb != null) {
            cb.run();
          }
        };
    if (delegate != null) {
      if (source != ConnectivityState.SHUTDOWN) {
        registerStateChangeCallback(once);
        if (isInFallbackMode() != initialFallbackMode) {
          unregisterStateChangeCallback(once);
          once.run();
          return;
        }
      }
      delegate.notifyWhenStateChanged(
          source,
          () -> {
            unregisterStateChangeCallback(once);
            once.run();
          });
    } else if (source != ConnectivityState.SHUTDOWN) {
      once.run();
    }
  }

  @Override
  public void resetConnectBackoff() {
    if (isInFallbackMode()) {
      if (fallbackDelegateChannel != null) {
        fallbackDelegateChannel.resetConnectBackoff();
      }
      return;
    }
    if (primaryDelegateChannel != null) {
      primaryDelegateChannel.resetConnectBackoff();
    }
  }

  @Override
  public void enterIdle() {
    if (isInFallbackMode()) {
      if (fallbackDelegateChannel != null) {
        fallbackDelegateChannel.enterIdle();
      }
      return;
    }
    if (primaryDelegateChannel != null) {
      primaryDelegateChannel.enterIdle();
    }
  }

  @Override
  public ManagedChannel shutdown() {
    synchronized (this) {
      isShutdown = true;
      stateChangeCallbacks.clear();
    }
    if (primaryProbeFuture != null) {
      primaryProbeFuture.cancel(false);
    }
    if (fallbackProbeFuture != null) {
      fallbackProbeFuture.cancel(false);
    }
    fallbackState.unregisterChannel(this);
    if (primaryDelegateChannel != null) {
      primaryDelegateChannel.shutdown();
    }
    if (fallbackDelegateChannel != null) {
      fallbackDelegateChannel.shutdown();
    }
    if (ownsFallbackState) {
      fallbackState.shutdown();
    }
    return this;
  }

  @Override
  public ManagedChannel shutdownNow() {
    synchronized (this) {
      isShutdown = true;
      stateChangeCallbacks.clear();
    }
    if (primaryProbeFuture != null) {
      primaryProbeFuture.cancel(true);
    }
    if (fallbackProbeFuture != null) {
      fallbackProbeFuture.cancel(true);
    }
    fallbackState.unregisterChannel(this);
    if (primaryDelegateChannel != null) {
      primaryDelegateChannel.shutdownNow();
    }
    if (fallbackDelegateChannel != null) {
      fallbackDelegateChannel.shutdownNow();
    }
    if (ownsFallbackState) {
      fallbackState.shutdownNow();
    }
    return this;
  }

  @Override
  public boolean isShutdown() {
    if (primaryDelegateChannel != null && !primaryDelegateChannel.isShutdown()) {
      return false;
    }

    if (fallbackDelegateChannel != null && !fallbackDelegateChannel.isShutdown()) {
      return false;
    }

    if (ownsFallbackState && fallbackState.ownsExecutor() && execService != null) {
      return execService.isShutdown();
    }
    return true;
  }

  @Override
  public boolean isTerminated() {
    if (primaryDelegateChannel != null && !primaryDelegateChannel.isTerminated()) {
      return false;
    }

    if (fallbackDelegateChannel != null && !fallbackDelegateChannel.isTerminated()) {
      return false;
    }

    if (ownsFallbackState && fallbackState.ownsExecutor() && execService != null) {
      return execService.isTerminated();
    }
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    if (primaryDelegateChannel != null) {
      boolean terminated = primaryDelegateChannel.awaitTermination(timeout, unit);
      if (!terminated) {
        return false;
      }
    }

    long awaitTimeNanos = endTimeNanos - System.nanoTime();
    if (fallbackDelegateChannel != null) {
      boolean terminated = fallbackDelegateChannel.awaitTermination(awaitTimeNanos, NANOSECONDS);
      if (!terminated) {
        return false;
      }
      awaitTimeNanos = endTimeNanos - System.nanoTime();
    }

    if (ownsFallbackState && fallbackState.ownsExecutor() && execService != null) {
      return execService.awaitTermination(awaitTimeNanos, NANOSECONDS);
    }
    return true;
  }
}
