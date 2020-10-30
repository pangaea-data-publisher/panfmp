package de.pangaea.metadataportal.processor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;

public class MinimalTransportClient extends TransportClient {
  static {
    initializeNetty();
  }
  
  /**
   * Netty wants to do some unwelcome things like use unsafe and replace a private field, or use a poorly considered buffer recycler. This
   * method disables these things by default, but can be overridden by setting the corresponding system properties.
   */
  private static void initializeNetty() {
    /*
     * We disable three pieces of Netty functionality here:
     *  - we disable Netty from being unsafe
     *  - we disable Netty from replacing the selector key set
     *  - we disable Netty from using the recycler
     *
     * While permissions are needed to read and set these, the permissions needed here are innocuous and thus should simply be granted
     * rather than us handling a security exception here.
     */
    setSystemPropertyIfUnset("io.netty.noUnsafe", Boolean.toString(true));
    setSystemPropertyIfUnset("io.netty.noKeySetOptimization", Boolean.toString(true));
    setSystemPropertyIfUnset("io.netty.recycler.maxCapacityPerThread", Integer.toString(0));
  }
  
  private static void setSystemPropertyIfUnset(final String key, final String value) {
    final String currentValue = System.getProperty(key);
    if (currentValue == null) {
      System.setProperty(key, value);
    }
  }
  
  private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS =
      Collections.unmodifiableList(Arrays.asList(
          Netty4Plugin.class
      ));
  
  /**
   * Creates a new transport client with pre-installed plugins.
   *
   * @param settings the settings passed to this transport client
   * @param plugins  an optional array of additional plugins to run with this client
   */
  @SafeVarargs
  public MinimalTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
    this(settings, Arrays.asList(plugins));
  }
  
  /**
   * Creates a new transport client with pre-installed plugins.
   *
   * @param settings the settings passed to this transport client
   * @param plugins  a collection of additional plugins to run with this client
   */
  public MinimalTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
    this(settings, plugins, null);
  }
  
  /**
   * Creates a new transport client with pre-installed plugins.
   *
   * @param settings            the settings passed to this transport client
   * @param plugins             a collection of additional plugins to run with this client
   * @param hostFailureListener a failure listener that is invoked if a node is disconnected; this can be <code>null</code>
   */
  public MinimalTransportClient(
      Settings settings,
      Collection<Class<? extends Plugin>> plugins,
      HostFailureListener hostFailureListener) {
    super(settings, Settings.EMPTY, addPlugins(plugins, PRE_INSTALLED_PLUGINS), hostFailureListener);
  }
    

  @Override
  public void close() {
    super.close();
    if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings) == false
        || NetworkModule.TRANSPORT_TYPE_SETTING.get(settings).equals(Netty4Plugin.NETTY_TRANSPORT_NAME)) {
      try {
        GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
