package com.abhioncbr.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import java.util.Map;


public class SalesforceSourceConnectorConfig extends AbstractConfig {
  // config variables properties.
  public static final String SF_KAFKA_CONNECT_VERSION_CONF  = "salesforce.version";

  public static final String SF_USERNAME_CONF       = "salesforce.username";
  public static final String SF_PASSWORD_CONF       = "salesforce.password";
  public static final String SF_CUSTOM_URL_CONF     = "salesforce.custom.url";
  public static final String SF_API_VERSION_CONF    = "salesforce.api.version";
  public static final String SF_CONNECT_TYPE_CONF   = "salesforce.connect.type";
  public static final String SF_CONNECTION_TIMEOUT_CONF = "salesforce.connection.timeout";

  public static final String SF_CDC_TOPICS_CONF     = "salesforce.cdc.topics";
  public static final String SF_PUSH_TOPICS_CONF    = "salesforce.push.topics";
  public static final String SF_KAFKA_TOPICS_CONF   = "salesforce.kafka.topic.regex";

  public static final String SF_NOTIFY_CREATE_CONF = "salesforce.notify.create";
  public static final String SF_NOTIFY_UPDATE_CONF = "salesforce.notify.update";
  public static final String SF_NOTIFY_DELETE_CONF = "salesforce.notify.delete";
  public static final String SF_NOTIFY_UNDELETE_CONF = "salesforce.notify.undelete";

  public static final String SF_LOG_SUCCESS_CONF = "salesforce.log.success";
  public static final String SF_LOG_FAILURE_CONF = "salesforce.log.failure";

  public SalesforceSourceConnectorConfig(Map<String, ?> parsedConfig) {
    super(conf(), parsedConfig);
    this.version = this.getString(SF_KAFKA_CONNECT_VERSION_CONF);

    this.username = this.getString(SF_USERNAME_CONF);
    this.password = this.getPassword(SF_PASSWORD_CONF).value();
    this.customURL = this.getString(SF_CUSTOM_URL_CONF);
    this.apiVersion = this.getString(SF_API_VERSION_CONF);
    this.connectType = ConnectType.valueOf(this.getString(SF_CONNECT_TYPE_CONF));
    this.connectTimeout = this.getLong(SF_CONNECTION_TIMEOUT_CONF);

    this.cdcTopics = this.getString(SF_CDC_TOPICS_CONF);
    this.pushTopics = this.getString(SF_PUSH_TOPICS_CONF);
    this.kafkaTopics = this.getString(SF_KAFKA_TOPICS_CONF);

    this.sfNotifyCreate = this.getBoolean(SF_NOTIFY_CREATE_CONF);
    this.sfNotifyUpdate = this.getBoolean(SF_NOTIFY_UPDATE_CONF);
    this.sfNotifyDelete = this.getBoolean(SF_NOTIFY_DELETE_CONF);
    this.sfNotifyUndelete = this.getBoolean(SF_NOTIFY_UNDELETE_CONF);

    this.sfLogSuccess  = this.getBoolean(SF_LOG_SUCCESS_CONF);
    this.sfLogFailure  = this.getBoolean(SF_LOG_FAILURE_CONF);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(SF_KAFKA_CONNECT_VERSION_CONF, Type.STRING, "0.1", Importance.HIGH, SF_KAFKA_CONNECT_VERSION_DOC)

        .define(SF_USERNAME_CONF,     Type.STRING, Importance.HIGH, SF_USERNAME_DOC)
        .define(SF_PASSWORD_CONF,     Type.PASSWORD, Importance.HIGH, SF_PASSWORD_DOC)
        .define(SF_CUSTOM_URL_CONF,   Type.STRING, Importance.HIGH, SF_CUSTOM_URL_DOC)
        .define(SF_API_VERSION_CONF,  Type.STRING, "47.0", Importance.HIGH, SF_API_VERSION_DOC)
        .define(SF_CONNECT_TYPE_CONF, Type.STRING, ConnectType.CDC.toString(), Importance.HIGH, SF_CONNECT_TYPE_DOC)
        .define(SF_CONNECTION_TIMEOUT_CONF, Type.LONG, 5L, Importance.HIGH, SF_CONNECTION_TIMEOUT_DOC)

        .define(SF_CDC_TOPICS_CONF,   Type.STRING, Importance.HIGH, SF_CDC_TOPICS_DOC)
        .define(SF_PUSH_TOPICS_CONF,  Type.STRING, Importance.HIGH, SF_PUSH_TOPICS_DOC)
        .define(SF_KAFKA_TOPICS_CONF, Type.STRING, Importance.HIGH, SF_KAFKA_TOPICS_DOC)

        .define(SF_NOTIFY_CREATE_CONF,   Type.BOOLEAN, true, Importance.LOW, SF_NOTIFY_CREATE_DOC)
        .define(SF_NOTIFY_UPDATE_CONF,   Type.BOOLEAN, true, Importance.LOW, SF_NOTIFY_UPDATE_DOC)
        .define(SF_NOTIFY_DELETE_CONF,   Type.BOOLEAN, true, Importance.LOW, SF_NOTIFY_DELETE_DOC)
        .define(SF_NOTIFY_UNDELETE_CONF, Type.BOOLEAN, true, Importance.LOW, SF_NOTIFY_UNDELETE_DOC)

        .define(SF_LOG_SUCCESS_CONF,   Type.BOOLEAN, true, Importance.LOW, SF_LOG_SUCCESS_DOC)
        .define(SF_LOG_FAILURE_CONF,   Type.BOOLEAN, true, Importance.LOW, SF_LOG_FAILURE_DOC);
  }

  //config variables.
  public final String version;

  public final String username;
  public final String password;
  public final String customURL;
  public final String apiVersion;
  public final ConnectType connectType;
  public final long connectTimeout;

  public final String cdcTopics;
  public final String pushTopics;
  public final String kafkaTopics;

  public final boolean sfNotifyCreate;
  public final boolean sfNotifyUpdate;
  public final boolean sfNotifyDelete;
  public final boolean sfNotifyUndelete;

  public final boolean sfLogSuccess;
  public final boolean sfLogFailure;

  // config variables docs.
  static final String SF_KAFKA_CONNECT_VERSION_DOC = "Salesforce Kafka connect version.";

  static final String SF_USERNAME_DOC = "Salesforce username to connect with.";
  static final String SF_PASSWORD_DOC = "Salesforce password to connect with.";
  static final String SF_CUSTOM_URL_DOC = "Salesforce custom url to connect with.";
  static final String SF_API_VERSION_DOC = "Salesforce api version to connect with.";
  static final String SF_CONNECT_TYPE_DOC = "Type of Salesforce method to get data. CDC or Push Topic.";
  static final String SF_CONNECTION_TIMEOUT_DOC = "The amount of time to wait while connecting to the Salesforce streaming endpoint.";

  static final String SF_CDC_TOPICS_DOC = "Salesforce CDC Subscriptions to populate to Kafka.";
  static final String SF_PUSH_TOPICS_DOC = "Salesforce Push Topics to populate to Kafka.";
  static final String SF_KAFKA_TOPICS_DOC = "Kafka topic to populate. ";

  static final String SF_NOTIFY_CREATE_DOC   = "Flag to determine if the PushTopic / CDC should respond to creates.";
  static final String SF_NOTIFY_UPDATE_DOC   = "Flag to determine if the PushTopic / CDC should respond to updates.";
  static final String SF_NOTIFY_DELETE_DOC   = "Flag to determine if the PushTopic / CDC should respond to deletes.";
  static final String SF_NOTIFY_UNDELETE_DOC = "Flag to determine if the PushTopic / CDC should respond to undeletes.";

  static final String SF_LOG_SUCCESS_DOC   = "Flag to determine if to log the success messages or not.";
  static final String SF_LOG_FAILURE_DOC   = "Flag to determine if to log the failure messages or not.";
}