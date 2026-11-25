package com.pedidos360.recomendaciones.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.recomendaciones.dto.EventoInteraccionDTO;
import com.pedidos360.recomendaciones.model.ProductoPopularidad;
import com.pedidos360.recomendaciones.repository.ProductoPopularidadRepositorio;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecomendacionesService {

    // Tópico de Kafka donde se publican las vistas y compras
    public static final String TOPIC_EVENTOS = "product-events";

    private final ProductoPopularidadRepositorio repositorio;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Envía el evento de interacción a Kafka como texto JSON
    public void registrarInteraccion(EventoInteraccionDTO evento) {
        try {
            if (evento.getFecha() == null) {
                evento.setFecha(LocalDateTime.now());
            }
            String json = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(TOPIC_EVENTOS, evento.getIdProducto(), json);
            log.info("Evento [{}] enviado a Kafka para producto: {}",
                    evento.getTipoEvento(), evento.getIdProducto());
        } catch (Exception e) {
            log.error("Error al serializar evento hacia Kafka: {}", e.getMessage());
            throw new RuntimeException("Error publicando evento en Kafka", e);
        }
    }

    // Devuelve el Top 5 global ordenado por puntaje
    public List<ProductoPopularidad> obtenerTop5General() {
        return repositorio.findTop5ByOrderByPuntajeDesc();
    }

    // Devuelve el Top 5 filtrado por categoría
    public List<ProductoPopularidad> obtenerTop5PorCategoria(String categoria) {
        return repositorio.findTop5ByCategoriaIgnoreCaseOrderByPuntajeDesc(categoria);
    }
}
