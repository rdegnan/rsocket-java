/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.plugins;

import io.rsocket.Payload;

/** JVM wide plugins for RSocket */
public class Plugins {
  private static PluginRegistry<Payload> DEFAULT = new PluginRegistry<>();

  private Plugins() {}

  public static void interceptConnection(DuplexConnectionInterceptor interceptor) {
    DEFAULT.addConnectionPlugin(interceptor);
  }

  public static void interceptClient(RSocketInterceptor<Payload> interceptor) {
    DEFAULT.addClientPlugin(interceptor);
  }

  public static <T extends Payload> void interceptServer(RSocketInterceptor<Payload> interceptor) {
    DEFAULT.addServerPlugin(interceptor);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Payload> PluginRegistry<T> defaultPlugins() {
    return (PluginRegistry<T>) DEFAULT;
  }
}
