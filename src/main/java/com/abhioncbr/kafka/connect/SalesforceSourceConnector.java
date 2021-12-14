/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.abhioncbr.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SalesforceSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SalesforceSourceConnector.class);

  private SalesforceSourceConnectorConfig config;
  private List<Map<String, String>> configs = new ArrayList<>();

  @Override
  public String version() {
    return config != null ? config.version: "0.1";
  }

  @Override
  public void start(Map<String, String> map) {
    config = new SalesforceSourceConnectorConfig(map);

    Map<String, String> taskSettings = new HashMap<>();
    taskSettings.putAll(map);
    configs.add(taskSettings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SalesforceSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return this.configs;
  }

  @Override
  public void stop() {
    //TODO: for graceful stop and taking advantage of connector metadata topics.
  }

  @Override
  public ConfigDef config() {
    return SalesforceSourceConnectorConfig.conf();
  }
}