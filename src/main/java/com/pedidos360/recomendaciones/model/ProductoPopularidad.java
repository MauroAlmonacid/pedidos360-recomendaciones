package com.pedidos360.recomendaciones.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "producto_popularidad")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductoPopularidad {

    // Identificador único del producto (ej: "PROD-101")
    @Id
    private String idProducto;

    // Nombre visible del producto (ej: "Audífonos Bluetooth Pro")
    private String nombreProducto;

    // Categoría del producto (ej: "Tecnologia", "Hogar")
    private String categoria;

    // Cantidad de vistas (+1 punto cada una)
    private int vistas;

    // Cantidad de compras (+5 puntos cada una)
    private int compras;

    // Puntaje acumulado para ordenar el Top 5
    private int puntaje;

    // Fecha de la última interacción
    private LocalDateTime actualizadoEn;
}
