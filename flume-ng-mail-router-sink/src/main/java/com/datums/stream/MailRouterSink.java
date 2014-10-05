package com.datums.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Flume-ng conditional mail router Sink
 * 
 */
public class MailRouterSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory.getLogger(MailRouterSink.class);

  private String componentName;

  private String sender;
  private String recipient;
  private String host;
  private int port;
  private String subject;
  List<String> subjectFields;
  private String message;
  List<String> messageFields;

  private Map<Pattern, String> conditionsList;
  private String conditionName;

  private ScheduledExecutorService mailingScheduler;
  private Map<Pattern, ConcurrentHashMap<String, CopyOnWriteArrayList<Event>>> multiEventPool = new HashMap<>(5);
  private ConcurrentHashMap<String, CopyOnWriteArrayList<Event>> defaultEventPool = new ConcurrentHashMap<>(30);

  private int mailingDelay;
  private int mailBatchLimit;

  private SinkCounter sinkCounter;

  private Callable<Void> scheduledSender = new Callable<Void>() {
    @Override
    public Void call() {
      aggregateEvent(defaultEventPool, recipient);
      for (Pattern key : conditionsList.keySet()) {
        aggregateEvent(multiEventPool.get(key), conditionsList.get(key));
      }
      mailingScheduler.schedule(this, mailingDelay, TimeUnit.MILLISECONDS);
      return null;
    }
  };

  private void aggregateEvent(ConcurrentHashMap<String, CopyOnWriteArrayList<Event>> currentEventPool, String recipient) {
    List<List<Event>> currentEventSnapshot = scanEventPool(currentEventPool);
    if (currentEventSnapshot != null && currentEventSnapshot.size() <= mailBatchLimit) {
      for (List<Event> currentGroup : currentEventSnapshot) {
        StringBuffer bodyBuffer = new StringBuffer(currentGroup.size() * 100);
        bodyBuffer.append("Please check events:");
        if (currentGroup.size() > mailBatchLimit) {
          for (Event currentEvent : currentGroup.subList(0, mailBatchLimit)) {
            bodyBuffer.append("\n\n==>".concat(new String(currentEvent.getBody())));
          }
          bodyBuffer.append("\n\nEvent amount exceeded batch limition.");
        } else {
          for (Event currentEvent : currentGroup) {
            bodyBuffer.append("\n\n==>".concat(new String(currentEvent.getBody())));
          }
        }
        try {
          sendEvent(currentGroup.get(0).getHeaders(), bodyBuffer.toString(), recipient);
        } catch (MessagingException exception) {
          logger.error("Mail scheduler can not send events.", exception);
        }
      }
    } else {
      try {
        Map<String, String> specialHeaders = new HashMap<>();
        for (String field : subjectFields) {
          specialHeaders.put(field, "FATAL");
        }
        sendEvent(specialHeaders, "Error or Exception amount exceeded limition.", recipient);
      } catch (MessagingException exception) {
        logger.error("Mail scheduler can not send events.", exception);
      }
    }
  }

  private void sendEvent(Map<String, String> sumHeaders, String body, String recipient)
      throws AddressException, MessagingException {
    Properties properties = System.getProperties();
    properties.setProperty("mail.smtp.host", host);
    properties.put("mail.smtp.port", port);

    Session session = Session.getDefaultInstance(properties);
    MimeMessage mimeMessage = new MimeMessage(session);
    mimeMessage.setFrom(new InternetAddress(sender));
    mimeMessage.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));

    // Set mail fields
    Map<String, String> headers = sumHeaders;
    String value;
    String mailSubject = subject;
    for (String field : subjectFields) {
      try {
        if (field.equals("body")) {
          value = new String(body);
        } else {
          value = new String(headers.get(field));
        }
      } catch (NullPointerException t) {
        value = "";
      }
      mailSubject = mailSubject.replace("%{" + field + "}", value);
    }

    String mailMessage = message;
    for (String field : messageFields) {

      try {
        if (field.equals("body")) {
          value = new String(body);
        } else {
          value = new String(headers.get(field));
        }
      } catch (NullPointerException t) {
        value = "";
      }
      mailMessage = mailMessage.replace("%{" + field + "}", value);
    }

    mimeMessage.setSubject(mailSubject);
    mimeMessage.setText(mailMessage);
    // Send message
    Transport.send(mimeMessage);
  }

  private List<List<Event>> scanEventPool(
      ConcurrentHashMap<String, CopyOnWriteArrayList<Event>> currentEventPool) {
    List<List<Event>> snapshot = new ArrayList<>(30);
    for (String currentKey : currentEventPool.keySet()) {
      List<Event> currentGroup = currentEventPool.remove(currentKey);
      if (currentGroup.size() > 0) {
        snapshot.add(currentGroup);
      }
    }
    return snapshot;
  }

  /**
   * This function is saving event to MAP, using CopyOnWriteArrayList to prevent
   * ConcurrentModifyException.
   * 
   * @param event
   */
  private void poolEvent(Event event,
      ConcurrentHashMap<String, CopyOnWriteArrayList<Event>> eventPool) {
    Map<String, String> headers = event.getHeaders();
    StringBuffer keyBuffer = new StringBuffer(64);
    keyBuffer.append("KEY");
    for (String field : subjectFields) {
      keyBuffer.append("-".concat(headers.get(field)));
    }
    String key = keyBuffer.toString();
    if (StringUtils.isNotBlank(key)) {
      if (eventPool.containsKey(key)) {
        CopyOnWriteArrayList<Event> currentEventGroup = eventPool.get(key);
        currentEventGroup.add(event);
      } else {
        CopyOnWriteArrayList<Event> newEventGroup = new CopyOnWriteArrayList<>();
        newEventGroup.add(event);
        eventPool.put(key, newEventGroup);
      }
    } else {
      logger.warn("Blank key of mailing event.");
    }
  }

  /**
   * process events
   */
  @Override
  public Status process() throws EventDeliveryException {
    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    try {
      txn.begin();
      Event event = ch.take();
      sinkCounter.incrementEventDrainAttemptCount();

      if (event == null) {
        txn.rollback();
        return Status.BACKOFF;
      }

      String label = (event.getHeaders().get(conditionName) != null) ? event
          .getHeaders().get("origin") : "default";
      boolean defaultFlg = true;
      for (Pattern pattern : conditionsList.keySet()) {
        if (pattern.matcher(label).matches()) {
          defaultFlg = false;
          poolEvent(event, multiEventPool.get(pattern));
          continue;
        }
      }
      if (defaultFlg) {
        poolEvent(event, defaultEventPool);
      }

      txn.commit();
      sinkCounter.incrementEventDrainSuccessCount();
      return Status.READY;
    } catch (Throwable error) {
      txn.rollback();
      logger.error("Unable to send mail.", error);
      if (error instanceof Error) {
        throw (Error) error;
      }
      return Status.BACKOFF;
    } finally {
      txn.close();
    }
  }

  @Override
  public synchronized void start() {
    sinkCounter.start();
    super.start();
    logger.info("Agile mail sink {} Job monitor started.", componentName);
    mailingScheduler = Executors.newSingleThreadScheduledExecutor();
    mailingScheduler.schedule(scheduledSender, mailingDelay, TimeUnit.MILLISECONDS);
    logger.info("Scheduled event sender started.");
  }

  @Override
  public synchronized void stop() {
    logger.info("Agile mail sinks {} Job monitor stopped.", componentName);
    sinkCounter.stop();
    mailingScheduler.shutdown();
    while (!mailingScheduler.isTerminated()) {
      try {
        Thread.sleep(MailRouterSinkConstants.THREA_DQUIT_DELAY);
      } catch (InterruptedException exception) {
        logger.error("Interrupted while waiting for Job to terminate.", exception);
        Throwables.propagate(exception);
      }
    }
    logger.info("Scheduled event sender stopped.");
    super.stop();
  }

  /**
   * Configurate mail properties
   */
  @Override
  public void configure(Context context) {
    String[] hostName = null;
    if (StringUtils.isNotBlank(context
        .getString(MailRouterSinkConstants.HOSTNAME))) {
      hostName = context.getString(MailRouterSinkConstants.HOSTNAME).split(":");
    }
    host = hostName[0];
    port = hostName.length == 2 ? Integer.parseInt(hostName[1])
        : MailRouterSinkConstants.DEFAULT_PORT;
    Preconditions.checkState(host != null, "Missing Param:"
        + MailRouterSinkConstants.HOSTNAME);

    if (StringUtils.isNotBlank(context
        .getString(MailRouterSinkConstants.RECIPIENT))) {
      recipient = context.getString(MailRouterSinkConstants.RECIPIENT);
    }
    Preconditions.checkState(recipient != null, "Missing Param:"
        + MailRouterSinkConstants.RECIPIENT);

    if (StringUtils.isNotBlank(context
        .getString(MailRouterSinkConstants.SENDER))) {
      sender = context.getString(MailRouterSinkConstants.SENDER);
    }
    Preconditions.checkState(sender != null, "Missing Param:"
        + MailRouterSinkConstants.SENDER);

    if (StringUtils.isNotBlank(context
        .getString(MailRouterSinkConstants.SUBJECT))) {
      subject = context.getString(MailRouterSinkConstants.SUBJECT);
    }
    Preconditions.checkState(subject != null, "Missing Param:"
        + MailRouterSinkConstants.SUBJECT);

    subjectFields = new ArrayList<String>();
    Matcher match = Pattern.compile("%\\{(.*?)\\}").matcher(subject);
    while (match.find()) {
      subjectFields.add(match.group(1));
      logger.info("Parsing subject field: {}", match.group(1));
    }

    if (StringUtils.isNotBlank(context
        .getString(MailRouterSinkConstants.MESSAGE))) {
      message = context.getString(MailRouterSinkConstants.MESSAGE);
    }
    Preconditions.checkState(message != null, "Missing Param:"
        + MailRouterSinkConstants.MESSAGE);

    messageFields = new ArrayList<String>();
    match = Pattern.compile("%\\{(.*?)\\}").matcher(message);
    while (match.find()) {
      messageFields.add(match.group(1));
      logger.info("Parsing message field: {}", match.group(1));
    }

    // Resolve conditions list
    if (StringUtils.isNotBlank(context.getString(MailRouterSinkConstants.CONDITIONS)) &&
        StringUtils.isNotBlank(context.getString(MailRouterSinkConstants.CONDITIONS_NAME))) {
      conditionName = context.getString(MailRouterSinkConstants.CONDITIONS_NAME);
      String[] conditions = context.getString(MailRouterSinkConstants.CONDITIONS).split(",");

      conditionsList = new HashMap<>(conditions.length);
      for (String eachCondition : conditions) {
        String currentKey = MailRouterSinkConstants.CONDITIONS + "." + eachCondition + "." + MailRouterSinkConstants.CONDITION_KEY;
        String currentValue = MailRouterSinkConstants.CONDITIONS + "." + eachCondition + "." + MailRouterSinkConstants.CONDITION_VALUE;

        Pattern key = null;
        if (StringUtils.isNotBlank(context.getString(currentKey))) {
          key = Pattern.compile(context.getString(currentKey));
        }
        String value = null;
        if (StringUtils.isNotBlank(context.getString(currentValue))) {
          value = context.getString(currentValue);
        }
        if (key != null && value != null) {
          conditionsList.put(key, value);
          ConcurrentHashMap<String, CopyOnWriteArrayList<Event>> currentEventPool = new ConcurrentHashMap<>(30);
          multiEventPool.put(key, currentEventPool);
          logger.info("Registered condition: {} <-> {}.", key, value);
        } else {
          logger.error("Current condition: {} can not be null.", eachCondition);
        }
      }
    }

    // Resolve pooling mail parameters
    Configurables.ensureRequiredNonNull(context,
        MailRouterSinkConstants.MAIL_POOL_DURATION,
        MailRouterSinkConstants.MAIL_POOL_BATCH,
        MailRouterSinkConstants.COMPONENT_NAME);
    try {

      componentName = context.getString(MailRouterSinkConstants.COMPONENT_NAME,
          getName());
      mailBatchLimit = context.getInteger(MailRouterSinkConstants.MAIL_POOL_BATCH, 20);
      mailingDelay = context.getInteger(MailRouterSinkConstants.MAIL_POOL_DURATION, 30);
    } catch (NumberFormatException e) {
      mailBatchLimit = 20;
      mailingDelay = 30;
      componentName = "MAIL_SINK";
      logger.warn("Unproper pooling parameter format for mail, " +
          MailRouterSinkConstants.MAIL_POOL_DURATION + ":{} and " +
          MailRouterSinkConstants.MAIL_POOL_BATCH + ":{}", mailingDelay, mailBatchLimit);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }
}
