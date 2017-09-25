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

package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.Frame;
import io.rsocket.Payload;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Payload}. This implementation is <b>not</b> thread-safe, and hence
 * any method can not be invoked concurrently.
 */
public class PayloadImpl implements Payload {

  public static final PayloadImpl EMPTY = new PayloadImpl(Unpooled.EMPTY_BUFFER, null);

  private final ByteBuf data;
  private final ByteBuf metadata;

  public PayloadImpl(Frame frame) {
    this.data = frame.serializeData();
    this.metadata = frame.hasMetadata() ? frame.serializeMetadata() : null;
  }

  public PayloadImpl(String data) {
    this(data, null);
  }

  public PayloadImpl(String data, @Nullable String metadata) {
    this.data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, data);
    this.metadata =
        metadata == null ? null : ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, metadata);
  }

  public PayloadImpl(byte[] data) {
    this(data, null);
  }

  public PayloadImpl(byte[] data, @Nullable byte[] metadata) {
    this.data = Unpooled.wrappedBuffer(data);
    this.metadata = metadata == null ? null : Unpooled.wrappedBuffer(metadata);
  }

  public PayloadImpl(ByteBuffer data) {
    this(data, null);
  }

  public PayloadImpl(ByteBuffer data, @Nullable ByteBuffer metadata) {
    this.data = Unpooled.wrappedBuffer(data);
    this.metadata = metadata == null ? null : Unpooled.wrappedBuffer(metadata);
  }

  public PayloadImpl(ByteBuf data) {
    this(data, null);
  }

  public PayloadImpl(ByteBuf data, @Nullable ByteBuf metadata) {
    this.data = Unpooled.copiedBuffer(data);
    this.metadata = metadata == null ? null : Unpooled.copiedBuffer(metadata);
  }

  @Override
  public boolean hasMetadata() {
    return metadata != null;
  }

  @Override
  public ByteBuf serializeMetadata() {
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
  }

  @Override
  public ByteBuf serializeData() {
    return data;
  }

  @Override
  public void dispose() {}

  /**
   * Static factory method for a text payload. Mainly looks better than "new PayloadImpl(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload textPayload(String data) {
    return new PayloadImpl(data);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new PayloadImpl(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload textPayload(String data, @Nullable String metadata) {
    return new PayloadImpl(data, metadata);
  }
}
