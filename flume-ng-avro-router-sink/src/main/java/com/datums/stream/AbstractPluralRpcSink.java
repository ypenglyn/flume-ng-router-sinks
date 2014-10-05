package com.datums.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AbstractPluralRpcSink extends AbstractSink
    implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(AbstractPluralRpcSink.class);
  private String hostname;
  private Integer port;
  private RpcClient client;
  private Properties clientProps;
  private SinkCounter sinkCounter;
  private int cxnResetInterval;
  private AtomicBoolean resetConnectionFlag;
  private AtomicBoolean resetPluralConnectionFlag;
  private final int DEFAULT_CXN_RESET_INTERVAL = 0;
  private final ScheduledExecutorService cxnResetExecutor = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
          .setNameFormat("Rpc Sink Reset Thread").build());
  private Map<Pattern, RpcClient> conditionsList;
  private String conditionName;
  private Map<Pattern, Properties> conditionsPropertiesList;
  private String componentName;

  @Override
  public void configure(Context context) {
    clientProps = new Properties();
    Configurables.ensureRequiredNonNull(context, RouterSinkConstants.COMPONENT_NAME);
    componentName = context.getString(RouterSinkConstants.COMPONENT_NAME);
    setName(componentName);

    hostname = context.getString("hostname");
    port = context.getInteger("port");

    Preconditions.checkState(hostname != null, "No hostname specified");
    Preconditions.checkState(port != null, "No port specified");

    clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    clientProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +
        "h1", hostname + ":" + port);

    for (Entry<String, String> entry : context.getParameters().entrySet()) {
      clientProps.setProperty(entry.getKey(), entry.getValue());
    }

    // Resolve conditions list
    if (StringUtils.isNotBlank(context.getString(RouterSinkConstants.CONDITIONS)) &&
        StringUtils.isNotBlank(context.getString(RouterSinkConstants.CONDITIONS_NAME))) {
      conditionName = context.getString(RouterSinkConstants.CONDITIONS_NAME);
      String[] conditions = context.getString(RouterSinkConstants.CONDITIONS).split(",");

      conditionsPropertiesList = new HashMap<>(conditions.length);
      for (String eachCondition : conditions) {
        String currentKey = RouterSinkConstants.CONDITIONS + "." + eachCondition + "." + RouterSinkConstants.CONDITION_KEY;
        String currentValue = RouterSinkConstants.CONDITIONS + "." + eachCondition + "." + RouterSinkConstants.CONDITION_VALUE;

        Pattern key = null;
        if (StringUtils.isNotBlank(context.getString(currentKey))) {
          key = Pattern.compile(context.getString(currentKey));
        }

        Properties currentProps = new Properties();
        String currentHostname = context.getString(currentValue + RouterSinkConstants.CONDITION_VALUE_HOST_SUFFIX);
        String currentPort = context.getString(currentValue + RouterSinkConstants.CONDITION_VALUE_PORT_SUFFIX);

        Preconditions.checkState(currentHostname != null, "No hostname specified for " + eachCondition);
        Preconditions.checkState(currentPort != null, "No port specified for " + eachCondition);

        currentProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        currentProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +
            "h1", currentHostname + ":" + currentPort);

        for (Entry<String, String> entry : context.getParameters().entrySet()) {
          currentProps.setProperty(entry.getKey(), entry.getValue());
        }
        if (key != null && currentProps != null) {
          conditionsPropertiesList.put(key, currentProps);
          logger.info("Registered condition: {} <-> {}.", key, currentHostname + ":" + currentPort);
        } else {
          logger.error("Current condition: {} can not be null.", eachCondition);
        }
      }
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName() + "Counter");
    }
    cxnResetInterval = context.getInteger("reset-connection-interval",
        DEFAULT_CXN_RESET_INTERVAL);
    if (cxnResetInterval == DEFAULT_CXN_RESET_INTERVAL) {
      logger.info("Connection reset is set to " + String.valueOf
          (DEFAULT_CXN_RESET_INTERVAL) + ". Will not reset connection to next " +
          "hop");
    }
  }

  /**
   * Returns a new {@linkplain RpcClient} instance configured using the given
   * {@linkplain Properties} object. This method is called whenever a new
   * connection needs to be created to the next hop.
   * 
   * @param props
   * @return
   */
  protected abstract RpcClient initializeRpcClient(Properties props);

  /**
   * Returns a map of new {@linkplain RpcClient} instances configured using
   * corresponding {@linkplain Properties} object.
   * 
   * @param propList
   * @return
   */
  protected abstract Map<Pattern, RpcClient> initializePluralRpcClient(Map<Pattern, Properties> propList);

  private void createMainConnection() throws FlumeException {
    logger.info("Plural Rpc sink {}: Building RpcClient with hostname: {}, port: {}",
        new Object[] { getName(), hostname, port });
    try {
      resetConnectionFlag = new AtomicBoolean(false);
      client = initializeRpcClient(clientProps);
      Preconditions.checkNotNull(client, "Rpc Client could not be " +
          "initialized. " + getName() + " could not be started");
      sinkCounter.incrementConnectionCreatedCount();
      if (cxnResetInterval > 0) {
        cxnResetExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            resetConnectionFlag.set(true);
          }
        }, cxnResetInterval, TimeUnit.SECONDS);
      }
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      if (ex instanceof FlumeException) {
        throw (FlumeException) ex;
      } else {
        throw new FlumeException(ex);
      }
    }
    logger.debug("Plural Rpc sink {}: Created RpcClient: {}", getName(), client);
  }

  private void createPluralConnection() throws FlumeException {
    logger.info("Plural Rpc sink {}: Building RpcClients", getName());
    try {
      resetPluralConnectionFlag = new AtomicBoolean(false);
      conditionsList = initializePluralRpcClient(conditionsPropertiesList);
      for (Entry<Pattern, RpcClient> entry : conditionsList.entrySet()) {
        Preconditions.checkNotNull(entry.getValue(), "Rpc Client {} could not be initialized. " +
            "{} could not be started", entry.getKey(), getName());
        sinkCounter.incrementConnectionCreatedCount();
      }
      if (cxnResetInterval > 0) {
        cxnResetExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            resetPluralConnectionFlag.set(true);
          }
        }, cxnResetInterval, TimeUnit.SECONDS);
      }
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      if (ex instanceof FlumeException) {
        throw (FlumeException) ex;
      } else {
        throw new FlumeException(ex);
      }
    }
    logger.debug("Plural Rpc sink {}: Created RpcClients", getName());

  }

  /**
   * If this function is called successively without calling {@see
   * #destroyConnection()}, only the first call has any effect.
   * 
   * @throws org.apache.flume.FlumeException
   *           if an RPC client connection could not be opened
   */
  private void createConnection() throws FlumeException {
    createMainConnection();
    createPluralConnection();
  }

  private void resetMainConnection() {
    try {
      destroyMainConnection();
      createMainConnection();
    } catch (Throwable throwable) {
      // Don't rethrow, else this runnable won't get scheduled again.
      logger.error("Error while trying to expire connection",
          throwable);
    }
  }

  private void resetPluralConnection() {
    try {
      destroyPluralConnection();
      createPluralConnection();
    } catch (Throwable throwable) {
      // Don't rethrow, else this runnable won't get scheduled again.
      logger.error("Error while trying to expire connection",
          throwable);
    }
  }

  private void destroyElementConnection(RpcClient client) {
    if (client != null) {
      logger.debug("Plural Rpc sink {} closing Rpc client: {}", getName(), client);
      try {
        client.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (FlumeException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Plural Rpc sink " + getName() + ": Attempt to close Rpc " +
            "client failed. Exception follows.", e);
      }
    }
    client = null;
  }

  private void destroyMainConnection() {
    destroyElementConnection(client);
  }

  private void destroyPluralConnection() {
    if (conditionsList != null) {
      for (RpcClient client : conditionsList.values()) {
        destroyElementConnection(client);
      }
    }
    conditionsList = null;
  }

  private void destroyConnection() {
    destroyMainConnection();
    destroyPluralConnection();
  }

  private void verifyMainConnection() throws FlumeException {
    if (client == null) {
      createMainConnection();
    } else if (!client.isActive()) {
      destroyMainConnection();
      createMainConnection();
    }
  }

  private void verifyPluralConnection() throws FlumeException {
    if (conditionsList == null) {
      createPluralConnection();
    } else {
      boolean needResetPluralFlg = false;
      for (RpcClient client : conditionsList.values()) {
        if (!client.isActive()) {
          needResetPluralFlg = true;
        }
      }
      if (needResetPluralFlg) {
        destroyPluralConnection();
        createPluralConnection();
      }
    }
  }

  /**
   * Ensure the connection exists and is active. If the connection is not
   * active, destroy it and recreate it.
   * 
   * @throws org.apache.flume.FlumeException
   *           If there are errors closing or opening the RPC connection.
   */
  private void verifyConnection() throws FlumeException {
    verifyMainConnection();
    verifyPluralConnection();
  }

  private String pluralRpcToString() {
    StringBuffer buffer = new StringBuffer();
    for (Entry<Pattern, Properties> entry : conditionsPropertiesList.entrySet()) {
      buffer.append(" { " + entry.getKey() + "<->" + entry.getValue().getProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX +
          "h1") + " } ");
    }
    return buffer.toString();
  }

  /**
   * The start() of RpcSink is more of an optimization that allows connection to
   * be created before the process() loop is started. In case it so happens that
   * the start failed, the process() loop will itself attempt to reconnect as
   * necessary. This is the expected behavior since it is possible that the
   * downstream source becomes unavailable in the middle of the process loop and
   * the sink will have to retry the connection again.
   */
  @Override
  public void start() {
    logger.info("Starting {}...", this);
    sinkCounter.start();
    try {
      createConnection();
    } catch (FlumeException e) {
      logger.warn("Unable to create Rpc client using hostname: " + hostname
          + ", port: " + port, e);

      /* Try to prevent leaking resources. */
      destroyConnection();
    }

    super.start();

    logger.info("Plural Rpc sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("Plural Rpc sink {} stopping...", getName());

    destroyConnection();
    cxnResetExecutor.shutdown();
    try {
      if (cxnResetExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cxnResetExecutor.shutdownNow();
      }
    } catch (Exception ex) {
      logger.error("Interrupted while waiting for connection reset executor " +
          "to shut down");
    }
    sinkCounter.stop();
    super.stop();

    logger.info("Plural Rpc sink {} stopped. Metrics: {}", getName(), sinkCounter);
  }

  @Override
  public String toString() {
    return "PluralRpcSink " + getName() + " { host: " + hostname + ", port: " +
        port + " } " + pluralRpcToString();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    if (resetConnectionFlag.get()) {
      resetMainConnection();
      // if the time to reset is long and the timeout is short
      // this may cancel the next reset request
      // this should however not be an issue
      resetConnectionFlag.set(false);
    }

    if (resetPluralConnectionFlag.get()) {
      resetPluralConnection();
      resetPluralConnectionFlag.set(false);
    }

    try {
      transaction.begin();
      // for (Entry<Pattern, RpcClient> entry : conditionsList.entrySet()) {
      // logger.info("----------->>>{}, {}", entry.getValue().isActive(),
      // client.isActive());
      // }
      verifyConnection();
      // for (Entry<Pattern, RpcClient> entry : conditionsList.entrySet()) {
      // logger.info("----------->>>{}, {}", entry.getValue().isActive(),
      // client.isActive());
      // }
      List<Event> batch = Lists.newLinkedList();
      Map<Pattern, List<Event>> pluralBatch = new HashMap<>();
      Map<Pattern, Integer> pluralSize = new HashMap<>();

      for (Pattern currentPattern : conditionsList.keySet()) {
        List<Event> currentBatch = Lists.newLinkedList();
        pluralBatch.put(currentPattern, currentBatch);
        pluralSize.put(currentPattern, new Integer(0));
      }

      for (int i = 0; i < client.getBatchSize(); i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }

        String label = (event.getHeaders().get(conditionName) != null) ?
            event.getHeaders().get(conditionName) : "default";
        boolean defaultFlg = true;
        for (Pattern pattern : conditionsList.keySet()) {
          if (pattern.matcher(label).matches()) {
            Integer sizeCounter = pluralSize.get(pattern);
            defaultFlg = false;
            pluralBatch.get(pattern).add(event);
            pluralSize.put(pattern, sizeCounter + 1);
            continue;
          }
        }
        if (defaultFlg) {
          batch.add(event);
        }
      }

      int size = batch.size();
      int batchSize = client.getBatchSize();
      int totalSize = size;
      for (Entry<Pattern, Integer> entry : pluralSize.entrySet()) {
        totalSize = totalSize + entry.getValue().intValue();
      }
      if (totalSize == 0) {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
      } else {
        if (totalSize < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }
        sinkCounter.addToEventDrainAttemptCount(totalSize);
        if (size == 0) {
          for (Entry<Pattern, RpcClient> entry : conditionsList.entrySet()) {
            entry.getValue().appendBatch(pluralBatch.get(entry.getKey()));
          }
        } else if (size == totalSize) {
          client.appendBatch(batch);
        } else {
          for (Entry<Pattern, RpcClient> entry : conditionsList.entrySet()) {
            entry.getValue().appendBatch(pluralBatch.get(entry.getKey()));
          }
          client.appendBatch(batch);
        }
      }

      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(size);

    } catch (Throwable t) {
      transaction.rollback();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof ChannelException) {
        logger.error("Rpc Sink " + getName() + ": Unable to get event from" +
            " channel " + channel.getName() + ". Exception follows.", t);
        status = Status.BACKOFF;
      } else {
        destroyConnection();
        throw new EventDeliveryException("Failed to send events", t);
      }
    } finally {
      transaction.close();
    }

    return status;
  }

  @VisibleForTesting
  RpcClient getUnderlyingClient() {
    return client;
  }

  @VisibleForTesting
  Map<Pattern, RpcClient> getUnderlyingPluralClient() {
    return conditionsList;
  }
}
