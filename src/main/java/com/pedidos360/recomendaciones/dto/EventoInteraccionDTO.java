package com.pedidos360.recomendaciones.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventoInteraccionDTO {

    // Cliente que interactuó con el producto
    private String idCliente;

    // Código del producto
    private String idProducto;

    // Nombre del producto
    private String nombreProducto;

    // Categoría a la que pertenece
    private String categoria;

    // Acción realizada: "VISTA" (+1 pt) o "COMPRA" (+5 pts)
    private String tipoEvento;

    // Timestamp del evento
    private LocalDateTime fecha;
}
