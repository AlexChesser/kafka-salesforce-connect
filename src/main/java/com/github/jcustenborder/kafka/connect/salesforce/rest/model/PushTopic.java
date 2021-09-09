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
package com.github.jcustenborder.kafka.connect.salesforce.rest.model;

import com.google.api.client.util.Key;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PushTopic {
  @Key("attributes")
  Map<String, Object> attributes;
  @Key("Name")
  String name;
  @Key("Query")
  String query;
  @Key("ApiVersion")
  BigDecimal apiVersion;
  @Key("NotifyForOperationCreate")
  Boolean notifyForOperationCreate;
  @Key("NotifyForOperationUpdate")
  Boolean notifyForOperationUpdate;
  @Key("NotifyForOperationUndelete")
  Boolean notifyForOperationUndelete;
  @Key("NotifyForOperationDelete")
  Boolean notifyForOperationDelete;
  @Key("NotifyForFields")
  String notifyForFields;

  public String name() {
    return this.name;
  }

  public void name(String name) {
    this.name = name;
  }

  public String query() {
    return this.query;
  }

  public void query(String query) {
    this.query = query;
  }

  public BigDecimal apiVersion() {
    return this.apiVersion;
  }

  public void apiVersion(BigDecimal apiVersion) {
    this.apiVersion = apiVersion;
  }

  public Boolean notifyForOperationCreate() {
    return this.notifyForOperationCreate;
  }

  public void notifyForOperationCreate(Boolean notifyForOperationCreate) {
    this.notifyForOperationCreate = notifyForOperationCreate;
  }

  public Boolean notifyForOperationUpdate() {
    return this.notifyForOperationUpdate;
  }

  public void notifyForOperationUpdate(Boolean notifyForOperationUpdate) {
    this.notifyForOperationUpdate = notifyForOperationUpdate;
  }

  public Boolean notifyForOperationUndelete() {
    return this.notifyForOperationUndelete;
  }

  public void notifyForOperationUndelete(Boolean notifyForOperationUndelete) {
    this.notifyForOperationUndelete = notifyForOperationUndelete;
  }

  public Boolean notifyForOperationDelete() {
    return this.notifyForOperationDelete;
  }

  public void notifyForOperationDelete(Boolean notifyForOperationDelete) {
    this.notifyForOperationDelete = notifyForOperationDelete;
  }

  public String notifyForFields() {
    return this.notifyForFields;
  }

  public void notifyForFields(String notifyForFields) {
    this.notifyForFields = notifyForFields;
  }

  public List<String> getSelectedFieldNames() {
    List<String> fieldNames = new ArrayList<String>();
    Matcher matcher = Pattern.compile("SELECT (.* )+FROM").matcher(this.query());
    if (matcher.find()) {
      String fieldsClause = matcher.group(1);

      Matcher innerMatcher = Pattern.compile("([A-Za-z0-9_]+),*").matcher(fieldsClause);
      while (innerMatcher.find()) {
        String oneField = innerMatcher.group(1);
        fieldNames.add(oneField);
      }
    }
    return fieldNames;
  }
}