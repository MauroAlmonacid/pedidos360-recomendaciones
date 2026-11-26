package com.pedidos360.recomendaciones.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration // Indica a Spring Boot que esta clase define configuraciones al arrancar
public class KafkaTopicConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConfig.class);

    // 1. Define explícitamente el tópico de recomendaciones con particiones, réplicas y retención de 7 días
    @Bean
    public NewTopic productEventsTopic() {
        return TopicBuilder.name("product-events")
                .partitions(3)                                  // Permite escalar hasta 3 consumidores en paralelo
                .replicas(1)                                    // Factor de replicación acorde a un broker único en EC2
                .config("retention.ms", "604800000")            // Conserva los eventos de vistas/compras por 7 días (en ms)
                .config("cleanup.policy", "delete")             // Elimina segmentos antiguos al vencer la retención
                .build();
    }

    // 2. Define explícitamente el tópico de pagos/antifraude con retención de 24 horas
    @Bean
    public NewTopic paymentAttemptsTopic() {
        return TopicBuilder.name("payment-attempts")
                .partitions(3)                                  // Balanceo de carga por cliente/transacción
                .replicas(1)                                    // Factor de replicación para el clúster actual
                .config("retention.ms", "86400000")             // Conserva intentos de pago durante 24 horas (en ms)
                .config("cleanup.policy", "delete")             // Limpia eventos expirados automáticamente
                .build();
    }

}