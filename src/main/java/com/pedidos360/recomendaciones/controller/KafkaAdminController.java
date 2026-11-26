package com.pedidos360.recomendaciones.controller;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/kafka-admin") // Ruta base para administrar tópicos y grupos consumidores
@CrossOrigin(origins = "*")      // Permite consultas desde tu frontend Angular y pruebas directas
public class KafkaAdminController {

    private final KafkaAdmin kafkaAdmin;

    // Spring Boot inyecta automáticamente el KafkaAdmin conectado a tu EC2 de Kafka
    public KafkaAdminController(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    // 1. LISTAR TÓPICOS ACTIVOS: GET /kafka-admin/topicos
    @GetMapping("/topicos")
    public ResponseEntity<?> listarTopicos() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> nombres = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "total", nombres.size(),
                    "topicos", nombres
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // 2. CREAR TÓPICO CON PARTICIONES Y RETENCIÓN: POST /kafka-admin/topicos
    @PostMapping("/topicos")
    public ResponseEntity<?> crearTopico(@RequestBody Map<String, Object> body) {
        String nombre = String.valueOf(body.getOrDefault("nombre", "")).trim();
        int particiones = Integer.parseInt(String.valueOf(body.getOrDefault("particiones", 3)));
        short replicas = Short.parseShort(String.valueOf(body.getOrDefault("replicas", 1)));
        String retencionMs = String.valueOf(body.getOrDefault("retentionMs", "604800000"));

        if (nombre.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "El campo 'nombre' es obligatorio"));
        }

        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic nuevoTopico = new NewTopic(nombre, particiones, replicas)
                    .configs(Map.of(
                            "retention.ms", retencionMs,
                            "cleanup.policy", "delete"
                    ));
            admin.createTopics(List.of(nuevoTopico)).all().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "mensaje", "Tópico creado correctamente en Kafka",
                    "topico", nombre,
                    "particiones", particiones,
                    "retentionMs", retencionMs
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // 3. MODIFICAR CONFIGURACIÓN (RETENCIÓN) DE UN TÓPICO: PUT /kafka-admin/topicos/{nombre}/retencion
    @PutMapping("/topicos/{nombre}/retencion")
    public ResponseEntity<?> actualizarRetencion(
            @PathVariable String nombre,
            @RequestBody Map<String, Object> body) {
        String nuevaRetencionMs = String.valueOf(body.getOrDefault("retentionMs", "86400000"));

        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            AlterConfigOp operacion = new AlterConfigOp(
                    new ConfigEntry("retention.ms", nuevaRetencionMs),
                    AlterConfigOp.OpType.SET
            );
            admin.incrementalAlterConfigs(Map.of(recurso, List.of(operacion))).all().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "mensaje", "Retención actualizada correctamente",
                    "topico", nombre,
                    "retentionMs", nuevaRetencionMs
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // 4. ELIMINAR TÓPICO: DELETE /kafka-admin/topicos/{nombre}
    @DeleteMapping("/topicos/{nombre}")
    public ResponseEntity<?> eliminarTopico(@PathVariable String nombre) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            admin.deleteTopics(List.of(nombre)).all().get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "mensaje", "Tópico eliminado de Kafka",
                    "topico", nombre
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // 5. INSPECCIONAR CONSUMER GROUPS Y SU ESTADO: GET /kafka-admin/consumer-groups
    @GetMapping("/consumer-groups")
    public ResponseEntity<?> listarConsumerGroups() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Collection<ConsumerGroupListing> listado = admin.listConsumerGroups().all().get(5, TimeUnit.SECONDS);
            List<String> ids = listado.stream().map(ConsumerGroupListing::groupId).toList();

            if (ids.isEmpty()) {
                return ResponseEntity.ok(List.of());
            }

            Map<String, ConsumerGroupDescription> detalles = admin.describeConsumerGroups(ids).all().get(5, TimeUnit.SECONDS);
            List<Object> resultado = new ArrayList<>();

            for (ConsumerGroupDescription desc : detalles.values()) {
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("groupId", desc.groupId());
                info.put("estado", desc.state().toString());
                info.put("miembrosActivos", desc.members().size());
                resultado.add(info);
            }
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}