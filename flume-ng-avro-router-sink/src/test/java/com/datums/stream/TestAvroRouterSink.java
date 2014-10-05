package com.datums.stream;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class TestAvroRouterSink {
  private static final Logger logger = LoggerFactory.getLogger(TestAvroRouterSink.class);
  private static final String hostname1 = "127.0.0.1";
  private static final Integer port1 = 34543;
  private static final String hostname2 = "127.0.0.1";
  private static final Integer port2 = 34544;
  private static final String condition = "origin";

  private AvroRouterSink sink;
  private Channel channel;

  @Before
  public void setUp() throws Exception {
    sink = new AvroRouterSink();
    channel = new MemoryChannel();

    Context context = new Context();
    context.put("hostname", hostname1);
    context.put("port", String.valueOf(port1));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("conditions", "test");
    context.put("conditions.test.if", "^AAA");
    context.put("conditions.test.then.hostname", hostname2);
    context.put("conditions.test.then.port", String.valueOf(port2));
    context.put("condition", condition);
    context.put("component.name", "testAvroRouterSink");

    sink.setChannel(channel);

    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
  }

  @Test
  public void testLifeCycle() throws InterruptedException, InstantiationException, IllegalAccessException {
    Server defaultServer = createServer(new MockAvroServer(hostname1, port1), hostname1, port1);
    Server secondServer = createServer(new MockAvroServer(hostname2, port2), hostname2, port2);

    defaultServer.start();
    secondServer.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.START_OR_ERROR, 5000));

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.STOP_OR_ERROR, 5000));

    defaultServer.close();
    secondServer.close();
  }

  @Test
  public void testProcess() throws InterruptedException, EventDeliveryException, InstantiationException, IllegalAccessException {
    Map<String, String> header1 = new HashMap<>();
    Map<String, String> header2 = new HashMap<>();
    header1.put(condition, "AAA");
    header2.put(condition, "BBB");

    Server defaultServer = createServer(new MockAvroServer(hostname1, port1), hostname1, port1);
    Server secondServer = createServer(new MockAvroServer(hostname2, port2), hostname2, port2);

    defaultServer.start();
    secondServer.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header1));
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header2));
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 10; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.STOP_OR_ERROR, 5000));

    defaultServer.close();
    secondServer.close();
  }

  @Test
  public void testTimeout() throws InterruptedException, EventDeliveryException, InstantiationException, IllegalAccessException {
    Map<String, String> header1 = new HashMap<>();
    Map<String, String> header2 = new HashMap<>();
    header1.put(condition, "AAA");
    header2.put(condition, "BBB");

    AtomicLong delay1 = new AtomicLong();
    AtomicLong delay2 = new AtomicLong();
    Server defaultServer = createServer(new DelayMockAvroServer(delay1, hostname1, port1), hostname1, port1);
    Server secondServer = createServer(new DelayMockAvroServer(delay2, hostname2, port2), hostname2, port2);

    defaultServer.start();
    secondServer.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.START_OR_ERROR, 5000));

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 2; i++) {
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header1));
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header2));
    }
    transaction.commit();
    transaction.close();

    // should throw EventDeliveryException due to connect timeout
    delay1.set(3000L);
    delay2.set(3000L);
    boolean threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to connect timeout.", ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to connect timeout", threw);

    // now, allow the connect handshake to happen
    delay1.set(0);
    delay2.set(0);
    sink.process();

    // should throw another EventDeliveryException due to request timeout
    delay1.set(4000L);
    delay2.set(4000L);
    threw = false;
    try {
      sink.process();
    } catch (EventDeliveryException ex) {
      logger.info("Correctly threw due to request timeout.", ex);
      threw = true;
    }

    Assert.assertTrue("Must throw due to request timeout", threw);

    // now, allow the connect handshake to happen again
    delay1.set(0);
    delay2.set(0);
    sink.process();

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.STOP_OR_ERROR, 5000));

    defaultServer.close();
    secondServer.close();
  }

  @Test
  public void testFailedConnect() throws InterruptedException, EventDeliveryException, InstantiationException, IllegalAccessException {
    Map<String, String> header1 = new HashMap<>();
    Map<String, String> header2 = new HashMap<>();
    header1.put(condition, "AAA");
    header2.put(condition, "BBB");

    Server defaultServer = createServer(new MockAvroServer(hostname1, port1), hostname1, port1);
    Server secondServer = createServer(new MockAvroServer(hostname2, port2), hostname2, port2);

    defaultServer.start();
    secondServer.start();

    sink.start();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.START_OR_ERROR, 5000));

    Thread.sleep(500L); // let socket startup
    defaultServer.close();
    Thread.sleep(500L); // sleep a little to let close happen

    Transaction transaction = channel.getTransaction();

    transaction.begin();
    for (int i = 0; i < 10; i++) {
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header1));
      channel.put(EventBuilder.withBody("event type " + i, Charsets.UTF_8, header2));
    }
    transaction.commit();
    transaction.close();

    for (int i = 0; i < 10; i++) {
      boolean threwException = false;
      try {
        sink.process();
      } catch (EventDeliveryException ex) {
        threwException = true;
      }
      Assert.assertTrue("Must throw EventDeliveryException if disconnected", threwException);
    }

    defaultServer = createServer(new MockAvroServer(hostname1, port1), hostname1, port1);
    defaultServer.start();

    for (int i = 0; i < 10; i++) {
      Sink.Status status = sink.process();
      Assert.assertEquals(Sink.Status.READY, status);
    }

    Assert.assertEquals(Sink.Status.BACKOFF, sink.process());

    sink.stop();
    Assert.assertTrue(LifecycleController.waitForOneOf(sink, LifecycleState.STOP_OR_ERROR, 5000));

    defaultServer.close();
    secondServer.close();
  }

  @Test
  public void testReset() throws Exception {
    Server defaultServer = createServer(new MockAvroServer(hostname1, port1), hostname1, port1);
    Server secondServer = createServer(new MockAvroServer(hostname2, port2), hostname2, port2);

    defaultServer.start();
    secondServer.start();
    AvroRouterSink currentSink = new AvroRouterSink();
    Context context = new Context();
    context.put("hostname", hostname1);
    context.put("port", String.valueOf(port1));
    context.put("batch-size", String.valueOf(2));
    context.put("reset-connection-interval", String.valueOf("5"));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("conditions", "test");
    context.put("conditions.test.if", "^AAA");
    context.put("conditions.test.then.hostname", hostname2);
    context.put("conditions.test.then.port", String.valueOf(port2));
    context.put("condition", condition);
    context.put("component.name", "testAvroRouterSink");

    currentSink.setChannel(channel);
    Configurables.configure(currentSink, context);
    currentSink.start();
    RpcClient defaultClient = currentSink.getUnderlyingClient();
    Map<Pattern, RpcClient> otherClients = currentSink.getUnderlyingPluralClient();
    Thread.sleep(6000);
    currentSink.process();
    // Make sure they are not the same object, connection should be reset
    Assert.assertFalse(defaultClient == currentSink.getUnderlyingClient());
    Assert.assertFalse(otherClients == currentSink.getUnderlyingPluralClient());
    currentSink.stop();

    context.put("hostname", hostname1);
    context.put("port", String.valueOf(port1));
    context.put("batch-size", String.valueOf(2));
    context.put("reset-connection-interval", String.valueOf("0"));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("conditions", "test");
    context.put("conditions.test.if", "^AAA");
    context.put("conditions.test.then.hostname", hostname2);
    context.put("conditions.test.then.port", String.valueOf(port2));
    context.put("condition", condition);
    context.put("component.name", "testAvroRouterSink");

    currentSink.setChannel(channel);
    Configurables.configure(currentSink, context);
    currentSink.start();
    defaultClient = currentSink.getUnderlyingClient();
    otherClients = currentSink.getUnderlyingPluralClient();
    Thread.sleep(6000);
    // Make sure they are not the same object, connection should be reset
    Assert.assertTrue(defaultClient == currentSink.getUnderlyingClient());
    Assert.assertTrue(otherClients == currentSink.getUnderlyingPluralClient());
    sink.stop();

    context.clear();
    context.put("hostname", hostname1);
    context.put("port", String.valueOf(port1));
    context.put("batch-size", String.valueOf(2));
    context.put("connect-timeout", String.valueOf(2000L));
    context.put("request-timeout", String.valueOf(3000L));
    context.put("conditions", "test");
    context.put("conditions.test.if", "^AAA");
    context.put("conditions.test.then.hostname", hostname2);
    context.put("conditions.test.then.port", String.valueOf(port2));
    context.put("condition", condition);
    context.put("component.name", "testAvroRouterSink");

    currentSink.setChannel(channel);
    Configurables.configure(currentSink, context);
    currentSink.start();
    defaultClient = currentSink.getUnderlyingClient();
    otherClients = currentSink.getUnderlyingPluralClient();
    Thread.sleep(6000);
    // Make sure they are not the same object, connection should be reset
    Assert.assertTrue(defaultClient == currentSink.getUnderlyingClient());
    Assert.assertTrue(otherClients == currentSink.getUnderlyingPluralClient());
    currentSink.stop();
    defaultServer.close();
    secondServer.close();
  }

  @After
  public void tearDown() throws Exception {
  }

  private Server createServer(AvroSourceProtocol protocol, String hostname, Integer port) throws IllegalAccessException, InstantiationException {
    Server server = new NettyServer(new SpecificResponder(AvroSourceProtocol.class, protocol),
        new InetSocketAddress(hostname, port));
    return server;
  }

  private static class MockAvroServer implements AvroSourceProtocol {
    private String hostname;
    private Integer port;

    public MockAvroServer(String hostname, Integer port) {
      this.hostname = hostname;
      this.port = port;
    }

    @Override
    public org.apache.flume.source.avro.Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.info("<{}:{}> Received event:{}", new Object[] { hostname, port, event });
      return org.apache.flume.source.avro.Status.OK;
    }

    @Override
    public org.apache.flume.source.avro.Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
      logger.info("<{}:{}> Received event batch:{}", new Object[] { hostname, port, events });
      return org.apache.flume.source.avro.Status.OK;
    }
  }

  private static class DelayMockAvroServer implements AvroSourceProtocol {
    private final AtomicLong delay;
    private String hostname;
    private Integer port;

    public DelayMockAvroServer(AtomicLong delay, String hostname, Integer port) {
      this.delay = delay;
      this.hostname = hostname;
      this.port = port;
    }

    private void sleep() throws AvroRemoteException {
      try {
        Thread.sleep(delay.get());
      } catch (InterruptedException e) {
        throw new AvroRemoteException("Interrupted while sleeping", e);
      }
    }

    @Override
    public org.apache.flume.source.avro.Status append(AvroFlumeEvent event) throws AvroRemoteException {
      logger.info("<{}:{}> Received event:{}; delaying for {}ms", new Object[] { hostname, port, event, delay });
      sleep();
      return org.apache.flume.source.avro.Status.OK;
    }

    @Override
    public org.apache.flume.source.avro.Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
      logger.info("<{}:{}> Received event batch:{}; delaying for {}ms", new Object[] { hostname, port, events, delay });
      sleep();
      return org.apache.flume.source.avro.Status.OK;
    }
  }
}
