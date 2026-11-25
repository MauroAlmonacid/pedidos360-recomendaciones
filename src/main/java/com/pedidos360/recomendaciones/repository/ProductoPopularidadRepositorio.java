package com.pedidos360.recomendaciones.repository;

import com.pedidos360.recomendaciones.model.ProductoPopularidad;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductoPopularidadRepositorio extends JpaRepository<ProductoPopularidad, String> {

    // Obtiene los 5 productos con mayor puntaje en toda la tienda
    List<ProductoPopularidad> findTop5ByOrderByPuntajeDesc();

    // Obtiene los 5 productos con mayor puntaje dentro de una categoría
    List<ProductoPopularidad> findTop5ByCategoriaIgnoreCaseOrderByPuntajeDesc(String categoria);
}
