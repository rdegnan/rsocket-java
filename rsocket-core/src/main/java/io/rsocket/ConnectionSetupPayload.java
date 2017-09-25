/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket;

import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_M;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.SetupFrameFlyweight;

/**
 * Exposed to server for determination of RequestHandler based on mime types and SETUP metadata/data
 */
public abstract class ConnectionSetupPayload implements Payload {

  public static final int NO_FLAGS = 0;
  public static final int HONOR_LEASE = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
  public static final int STRICT_INTERPRETATION = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;

  public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType) {
    return new ConnectionSetupPayloadImpl(
        metadataMimeType, dataMimeType, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, NO_FLAGS);
  }

  public static ConnectionSetupPayload create(
      String metadataMimeType, String dataMimeType, Payload payload) {
    return new ConnectionSetupPayloadImpl(
        metadataMimeType,
        dataMimeType,
        payload.serializeData(),
        payload.serializeMetadata(),
        payload.hasMetadata() ? FLAGS_M : 0);
  }

  public static ConnectionSetupPayload create(
      String metadataMimeType, String dataMimeType, int flags) {
    return new ConnectionSetupPayloadImpl(
        metadataMimeType, dataMimeType, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, flags);
  }

  public static ConnectionSetupPayload create(final Frame setupFrame) {
    Frame.ensureFrameType(FrameType.SETUP, setupFrame);
    return new ConnectionSetupPayloadImpl(
        Frame.Setup.metadataMimeType(setupFrame),
        Frame.Setup.dataMimeType(setupFrame),
        setupFrame.serializeData(),
        setupFrame.serializeMetadata(),
        Frame.Setup.getFlags(setupFrame));
  }

  public abstract String metadataMimeType();

  public abstract String dataMimeType();

  public abstract int getFlags();

  public boolean willClientHonorLease() {
    return Frame.isFlagSet(getFlags(), HONOR_LEASE);
  }

  public boolean doesClientRequestStrictInterpretation() {
    return STRICT_INTERPRETATION == (getFlags() & STRICT_INTERPRETATION);
  }

  @Override
  public boolean hasMetadata() {
    return Frame.isFlagSet(getFlags(), FLAGS_M);
  }

  private static final class ConnectionSetupPayloadImpl extends ConnectionSetupPayload {

    private final String metadataMimeType;
    private final String dataMimeType;
    private final ByteBuf data;
    private final ByteBuf metadata;
    private final int flags;

    public ConnectionSetupPayloadImpl(
        String metadataMimeType, String dataMimeType, ByteBuf data, ByteBuf metadata, int flags) {
      this.metadataMimeType = metadataMimeType;
      this.dataMimeType = dataMimeType;
      this.data = data;
      this.metadata = metadata;
      this.flags = flags;

      if (!hasMetadata() && metadata.readableBytes() > 0) {
        throw new IllegalArgumentException("metadata flag incorrect");
      }
    }

    @Override
    public String metadataMimeType() {
      return metadataMimeType;
    }

    @Override
    public String dataMimeType() {
      return dataMimeType;
    }

    @Override
    public ByteBuf serializeMetadata() {
      return metadata;
    }

    @Override
    public ByteBuf serializeData() {
      return data;
    }

    @Override
    public int getFlags() {
      return flags;
    }

    @Override
    public void dispose() {}
  }
}
