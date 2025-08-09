package com.github.emcegom.jaras.play.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class JsonUtilsTest {
    static class User {
        private String name;
        private int age;
        private LocalDate birthday;
        private LocalDateTime createdAt;

        public User() {}

        public User(String name, int age, LocalDate birthday, LocalDateTime createdAt) {
            this.name = name;
            this.age = age;
            this.birthday = birthday;
            this.createdAt = createdAt;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
        public LocalDate getBirthday() { return birthday; }
        public void setBirthday(LocalDate birthday) { this.birthday = birthday; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    }

    @Test
    void testObjectSerializationAndDeserialization() {
        User user = new User("Alice", 30, LocalDate.of(1995, 8, 9), LocalDateTime.of(2025, 8, 9, 14, 30, 0));

        String json = JsonUtils.toJson(user);
        Assertions.assertTrue(json.contains("\"name\":\"Alice\""));
        Assertions.assertTrue(json.contains("\"birthday\":\"1995-08-09\""));
        Assertions.assertTrue(json.contains("\"createdAt\":\"2025-08-09 14:30:00\""));

        User deserialized = JsonUtils.fromJson(json, User.class);
        Assertions.assertEquals("Alice", deserialized.getName());
        Assertions.assertEquals(30, deserialized.getAge());
        Assertions.assertEquals(LocalDate.of(1995, 8, 9), deserialized.getBirthday());
        Assertions.assertEquals(LocalDateTime.of(2025, 8, 9, 14, 30, 0), deserialized.getCreatedAt());
    }

    @Test
    void testListDeserialization() {
        String json = "[{\"name\":\"Bob\",\"age\":25,\"birthday\":\"2000-01-01\",\"createdAt\":\"2025-08-09 14:30:00\"}]";

        List<User> users = JsonUtils.fromJsonList(json, User.class);
        Assertions.assertEquals(1, users.size());
        Assertions.assertEquals("Bob", users.get(0).getName());
        Assertions.assertEquals(LocalDate.of(2000, 1, 1), users.get(0).getBirthday());
    }

    @Test
    void testComplexTypeDeserialization() {
        String json = "{\"group1\":[{\"name\":\"Charlie\",\"age\":28,\"birthday\":\"1997-05-10\",\"createdAt\":\"2025-08-09 14:30:00\"}]}";

        Map<String, List<User>> data = JsonUtils.fromJson(json, new com.fasterxml.jackson.core.type.TypeReference<>() {});
        Assertions.assertTrue(data.containsKey("group1"));
        Assertions.assertEquals("Charlie", data.get("group1").get(0).getName());
    }
}
