package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;

public class EmptyPayload implements Payload {
  private static final EmptyPayload EMPTY_PAYLOAD = new EmptyPayload();

  private EmptyPayload() {}

  public static Payload instance() {
    return EMPTY_PAYLOAD;
  }

  @Override
  public boolean hasMetadata() {
    return false;
  }

  @Override
  public ByteBuf serializeMetadata() {
    return Unpooled.EMPTY_BUFFER;
  }

  @Override
  public ByteBuf serializeData() {
    return Unpooled.EMPTY_BUFFER;
  }
}
