package com.pedidos360.recomendaciones.controller;

import com.pedidos360.recomendaciones.dto.EventoInteraccionDTO;
import com.pedidos360.recomendaciones.model.ProductoPopularidad;
import com.pedidos360.recomendaciones.service.RecomendacionesService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/recomendaciones")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class RecomendacionesController {

    // Servicio que publica eventos en Kafka y consulta el ranking Top 5
    private final RecomendacionesService recomendacionesService;

    // Recibe por POST una vista o compra de producto y la envía a Kafka
    @PostMapping("/eventos")
    public ResponseEntity<Object> registrarEvento(@RequestBody EventoInteraccionDTO evento) {
        // Valida que la petición traiga el identificador del producto
        if (evento.getIdProducto() == null || evento.getIdProducto().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("message", "El campo idProducto es obligatorio."));
        }

        // Envía el evento al tópico "product-events" de Kafka
        recomendacionesService.registrarInteraccion(evento);

        // Responde HTTP 200 confirmando que el evento entró al flujo
        return ResponseEntity.ok(Map.of(
                "message", "Evento registrado y enviado a Kafka correctamente",
                "idProducto", evento.getIdProducto()
        ));
    }

    // Devuelve por GET los 5 productos con mayor puntaje en toda la tienda
    @GetMapping("/top")
    public ResponseEntity<List<ProductoPopularidad>> obtenerTopGeneral() {
        return ResponseEntity.ok(recomendacionesService.obtenerTop5General());
    }

    // Devuelve por GET los 5 productos con mayor puntaje dentro de una categoría
    @GetMapping("/top/{categoria}")
    public ResponseEntity<List<ProductoPopularidad>> obtenerTopPorCategoria(@PathVariable String categoria) {
        return ResponseEntity.ok(recomendacionesService.obtenerTop5PorCategoria(categoria));
    }
}