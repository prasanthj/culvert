/*
 * Copyright 2018 Prasanth Jayachandran
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
package com.github.prasanthj.culvert.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.github.javafaker.Faker;

/**
 *
 */
public class Column {
  enum Type {
    BOOLEAN,
    STRING,
    STRING_DICT,
    STRING_IP_ADDRESS,
    STRING_UUID_DICT,
    LONG,
    DOUBLE,
    TIMESTAMP,
    INT_YEAR,
    INT_MONTH
  }

  private String name;
  private Type type;
  private Object[] dictionary;
  private Faker faker;
  private Random random;
  private static List<String> UUIDs = new ArrayList<>();

  static {
    for (int i = 0; i < 1_000_000; i++) {
      UUIDs.add(UUID.randomUUID().toString());
    }
  }

  private Column(String name, Type type, Object[] dictionary) {
    this.name = name;
    this.type = type;
    this.dictionary = dictionary;
    this.random = new Random(123);
    this.faker = new Faker(random);
  }

  public static class ColumnBuilder {
    private String name;
    private Type type;
    private Object[] dictionary;

    public ColumnBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public ColumnBuilder withType(Type type) {
      this.type = type;
      return this;
    }

    public ColumnBuilder withDictionary(Object[] dictionary) {
      this.dictionary = dictionary;
      return this;
    }

    public Column build() {
      return new Column(name, type, dictionary);
    }
  }

  public static ColumnBuilder newBuilder() {
    return new ColumnBuilder();
  }

  public Object getValue() {
    switch (type) {
      case BOOLEAN:
        return random.nextBoolean();
      case LONG:
        return random.nextLong();
      case DOUBLE:
        return random.nextDouble();
      case TIMESTAMP:
        return faker.date().birthday().toInstant().toString();
      case STRING:
        return faker.name().fullName();
      case STRING_DICT:
        if (dictionary != null) {
          int randIdx = random.nextInt(dictionary.length);
          return dictionary[randIdx];
        }
        // if dictionary unspecified use colors
        return faker.color().name();
      case STRING_IP_ADDRESS:
        return faker.internet().ipV4Address();
      case STRING_UUID_DICT:
        return UUIDs.get(random.nextInt(UUIDs.size()));
      case INT_YEAR:
        return 2000 + (faker.date().birthday().getYear() % 50);
      case INT_MONTH:
        return faker.date().birthday().getMonth() % 30; // 50 * 30 partitions max
      default:
        return faker.chuckNorris().fact();
    }
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public Object[] getDictionary() {
    return dictionary;
  }
}
