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

import com.google.cloud.spanner.SpannerOptions.CallCredentialsProvider;
import com.google.cloud.spanner.XGoogSpannerRequestId.RequestIdCreator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.spanner.v1.GetSessionRequest;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Probes a primary sub-channel during DirectPath fallback recovery by executing {@code GetSession}
 * with a live multiplexed session from {@link ChannelSessionRegistry} and verifying that the
 * underlying transport is DirectPath ALTS ({@link Grpc#TRANSPORT_ATTR_SSL_SESSION} is {@code
 * null}).
 */
final class DirectPathFallbackProber implements Function<Channel, String> {
  static final Duration DEFAULT_PROBE_DEADLINE = Duration.ofSeconds(5);

  private final ChannelSessionRegistry sessionRegistry;
  private final SpannerMetadataProvider metadataProvider;
  private final String projectName;
  private final RequestIdCreator requestIdCreator;
  @Nullable private final CallCredentialsProvider callCredentialsProvider;
  private final Duration rpcDeadline;

  DirectPathFallbackProber(
      ChannelSessionRegistry sessionRegistry,
      SpannerMetadataProvider metadataProvider,
      String projectName,
      RequestIdCreator requestIdCreator,
      @Nullable CallCredentialsProvider callCredentialsProvider,
      Duration rpcDeadline) {
    this.sessionRegistry = Preconditions.checkNotNull(sessionRegistry);
    this.metadataProvider = Preconditions.checkNotNull(metadataProvider);
    this.projectName = Preconditions.checkNotNull(projectName);
    this.requestIdCreator = Preconditions.checkNotNull(requestIdCreator);
    this.callCredentialsProvider = callCredentialsProvider;
    Preconditions.checkArgument(
        rpcDeadline != null && !rpcDeadline.isZero() && !rpcDeadline.isNegative(),
        "rpcDeadline must be positive");
    this.rpcDeadline = rpcDeadline;
  }

  @Override
  public String apply(Channel channel) {
    return probe(channel);
  }

  /**
   * Executes a {@code GetSession} probe on {@code channel} using a registered multiplexed session.
   * Returns {@code ""} when the probe succeeds over DirectPath, {@code "WAITING_FOR_REAL_SESSION"}
   * when no session is available yet, {@code "NOT_DIRECTPATH"} if the call completed over standard
   * TLS/SSL instead of DirectPath ALTS, or the gRPC status code name on failure.
   */
  String probe(Channel channel) {
    String sessionName = sessionRegistry.nextSessionName();
    if (sessionName == null) {
      return "WAITING_FOR_REAL_SESSION";
    }

    ListenableFuture<Session> future = null;
    try {
      GetSessionRequest request = GetSessionRequest.newBuilder().setName(sessionName).build();
      CallOptions callOptions =
          CallOptions.DEFAULT.withDeadlineAfter(rpcDeadline.toNanos(), TimeUnit.NANOSECONDS);
      if (callCredentialsProvider != null) {
        CallCredentials callCredentials = callCredentialsProvider.getCallCredentials();
        if (callCredentials != null) {
          callOptions = callOptions.withCallCredentials(callCredentials);
        }
      }

      AtomicReference<Attributes> capturedAttributes = new AtomicReference<>();
      Channel channelWithHeaders =
          ClientInterceptors.intercept(
              channel,
              new ProbeHeaderAndAttributeInterceptor(newHeaders(sessionName), capturedAttributes));
      ClientCall<GetSessionRequest, Session> call =
          channelWithHeaders.newCall(SpannerGrpc.getGetSessionMethod(), callOptions);

      future = ClientCalls.futureUnaryCall(call, request);
      future.get(rpcDeadline.toNanos(), TimeUnit.NANOSECONDS);

      Attributes attrs = capturedAttributes.get();
      if (attrs != null && attrs.get(Grpc.TRANSPORT_ATTR_SSL_SESSION) != null) {
        return "NOT_DIRECTPATH";
      }
      return "";
    } catch (ExecutionException e) {
      Status status = Status.fromThrowable(e.getCause());
      return status.getCode().name();
    } catch (TimeoutException e) {
      if (future != null) {
        future.cancel(true);
      }
      return Status.Code.DEADLINE_EXCEEDED.name();
    } catch (InterruptedException e) {
      if (future != null) {
        future.cancel(true);
      }
      Thread.currentThread().interrupt();
      return "ERROR";
    } catch (Exception e) {
      return "ERROR";
    }
  }

  @VisibleForTesting
  Metadata newHeaders(String sessionName) {
    return DynamicChannelPoolPrimer.newCallHeaders(
        metadataProvider, projectName, requestIdCreator, sessionName, "name=");
  }

  private static final class ProbeHeaderAndAttributeInterceptor implements ClientInterceptor {
    private final Metadata headers;
    private final AtomicReference<Attributes> capturedAttributes;

    private ProbeHeaderAndAttributeInterceptor(
        Metadata headers, AtomicReference<Attributes> capturedAttributes) {
      this.headers = headers;
      this.capturedAttributes = capturedAttributes;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(call) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata requestHeaders) {
          requestHeaders.merge(headers);
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onHeaders(Metadata responseHeaders) {
                  capturedAttributes.set(call.getAttributes());
                  super.onHeaders(responseHeaders);
                }
              },
              requestHeaders);
        }
      };
    }
  }
}
