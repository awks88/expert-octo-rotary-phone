package com.smarthome.devicehandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class DeviceHandlerApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeviceHandlerApplication.class, args);
    }

    // ===== Модели =====
    static class DeviceCommand {
        private String command;
        public String getCommand() { return command; }
        public void setCommand(String command) { this.command = command; }
    }

    static class DeviceStatus {
        public String deviceId, status, lastCommand;
        public DeviceStatus(String deviceId, String status, String lastCommand) {
            this.deviceId = deviceId;
            this.status = status;
            this.lastCommand = lastCommand;
        }
    }

    // ===== Контроллер =====
    @RestController
    @RequestMapping("/api/device")
    static class DeviceController {

        private final Map<String, String> states = new ConcurrentHashMap<>();

        @GetMapping("/{id}/status")
        public ResponseEntity<DeviceStatus> getStatus(@PathVariable String id) {
            String state = states.getOrDefault(id, "offline");
            String lastCmd = states.getOrDefault(id + ":cmd", "none");
            return ResponseEntity.ok(new DeviceStatus(id, state, lastCmd));
        }

        @PostMapping("/{id}/command")
        public ResponseEntity<Map<String, String>> sendCommand(
                @PathVariable String id,
                @RequestBody DeviceCommand cmd) {

            if ("turn_on".equals(cmd.getCommand())) {
                states.put(id, "online");
            } else if ("turn_off".equals(cmd.getCommand())) {
                states.put(id, "offline");
            } else {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Unknown command"));
            }
            states.put(id + ":cmd", cmd.getCommand());
            return ResponseEntity.ok(Map.of("status", "accepted", "command", cmd.getCommand()));
        }

        @GetMapping("/{id}/data")
        public ResponseEntity<Map<String, Object>> getData(@PathVariable String id) {
            return ResponseEntity.ok(Map.of(
                    "deviceId", id,
                    "temperature", 20 + Math.random() * 10,
                    "humidity", 40 + Math.random() * 20
            ));
        }
    }
}