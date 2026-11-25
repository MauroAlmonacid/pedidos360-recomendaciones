package com.pedidos360.recomendaciones.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.recomendaciones.dto.EventoInteraccionDTO;
import com.pedidos360.recomendaciones.model.ProductoPopularidad;
import com.pedidos360.recomendaciones.repository.ProductoPopularidadRepositorio;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumidorEventosComponent {

    private final ProductoPopularidadRepositorio repositorio;
    private final ObjectMapper objectMapper;

    // Escucha cada evento del tópico "product-events" y actualiza el puntaje en H2
    @KafkaListener(topics = "product-events", groupId = "recomendaciones-group")
    @Transactional
    public void consumirEventoProducto(String mensajeJson) {
        EventoInteraccionDTO evento;
        try {
            evento = objectMapper.readValue(mensajeJson, EventoInteraccionDTO.class);
        } catch (Exception e) {
            log.error("JSON inválido recibido en product-events: {}", e.getMessage());
            return;
        }

        if (evento == null || evento.getIdProducto() == null || evento.getIdProducto().isBlank()) {
            log.warn("Payload sin idProducto ignorado.");
            return;
        }

        // Busca el producto en H2 o crea uno nuevo si es su primera interacción
        ProductoPopularidad producto = repositorio.findById(evento.getIdProducto())
                .orElseGet(() -> new ProductoPopularidad(
                        evento.getIdProducto(),
                        evento.getNombreProducto() != null ? evento.getNombreProducto() : "Producto " + evento.getIdProducto(),
                        evento.getCategoria() != null ? evento.getCategoria() : "General",
                        0,
                        0,
                        0,
                        LocalDateTime.now()
                ));

        if (evento.getNombreProducto() != null && !evento.getNombreProducto().isBlank()) {
            producto.setNombreProducto(evento.getNombreProducto());
        }
        if (evento.getCategoria() != null && !evento.getCategoria().isBlank()) {
            producto.setCategoria(evento.getCategoria());
        }

        // Regla de negocio: COMPRA suma 5 puntos, VISTA suma 1 punto
        String tipo = (evento.getTipoEvento() != null) ? evento.getTipoEvento().toUpperCase() : "VISTA";
        if ("COMPRA".equals(tipo)) {
            producto.setCompras(producto.getCompras() + 1);
            producto.setPuntaje(producto.getPuntaje() + 5);
        } else {
            producto.setVistas(producto.getVistas() + 1);
            producto.setPuntaje(producto.getPuntaje() + 1);
        }

        producto.setActualizadoEn(LocalDateTime.now());
        repositorio.save(producto);

        log.info("Ranking actualizado -> {} | Vistas: {} | Compras: {} | Puntaje: {}",
                producto.getNombreProducto(), producto.getVistas(), producto.getCompras(), producto.getPuntaje());
    }
}
