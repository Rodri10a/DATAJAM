-- ============================================
-- DataJam - Team ProductDetails
-- Sprint de Consultas SQL de Negocio
-- ============================================

USE datajam;

-- ============================================
-- NIVEL BASICO
-- ============================================

-- 1. Volumen: Cantidad total de pedidos y monto historico total recaudado
SELECT
    COUNT(*) AS total_pedidos,
    ROUND(SUM(total_amount), 2) AS monto_total_recaudado
FROM orders;


-- 2. Filtro: Top 15 productos mas caros
SELECT id, name, price
FROM products
ORDER BY price DESC
LIMIT 15;


-- 3. Primer Cruce: Nombre de usuarios con la fecha de su primer pedido
SELECT u.name, MIN(o.order_date) AS primer_pedido
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name
ORDER BY primer_pedido;


-- ============================================
-- NIVEL INTERMEDIO
-- ============================================

-- 4. Ticket Promedio: Promedio de gasto por pedido en 2024
SELECT ROUND(AVG(total_amount), 2) AS ticket_promedio_2024
FROM orders
WHERE YEAR(order_date) = 2024;


-- 5. Best Sellers: Top 5 productos con mas unidades vendidas
SELECT p.id, p.name, SUM(oi.quantity) AS unidades_vendidas
FROM products p
JOIN order_items oi ON p.id = oi.product_id
GROUP BY p.id, p.name
ORDER BY unidades_vendidas DESC
LIMIT 5;


-- 6. Usuarios Inactivos: Usuarios que nunca compraron
SELECT u.name, u.email
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE o.id IS NULL;


-- ============================================
-- NIVEL AVANZADO
-- ============================================

-- 7. Analisis de Lineas: Validar si la suma de (quantity * unit_price)
--    coincide con el total_amount de la cabecera
SELECT
    o.id AS order_id,
    COUNT(DISTINCT oi.product_id) AS items_distintos,
    o.total_amount AS total_cabecera,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_calculado,
    CASE
        WHEN ROUND(SUM(oi.quantity * oi.unit_price), 2) = ROUND(o.total_amount, 2)
        THEN 'COINCIDE'
        ELSE 'NO COINCIDE'
    END AS validacion
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.id, o.total_amount;


-- 8. Impacto de la Integracion: Productos agrupados por rating (product_details)
--    Muestra cuantas unidades se vendieron por rango de rating
SELECT
    CASE
        WHEN pd.rating >= 4.5 THEN '4.5 - 5.0 (Excelente)'
        WHEN pd.rating >= 4.0 THEN '4.0 - 4.4 (Muy Bueno)'
        WHEN pd.rating >= 3.0 THEN '3.0 - 3.9 (Bueno)'
        ELSE '< 3.0 (Regular)'
    END AS rango_rating,
    COUNT(DISTINCT p.id) AS cantidad_productos,
    SUM(oi.quantity) AS unidades_vendidas,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue_total
FROM products p
JOIN product_details pd ON p.id = pd.product_id
JOIN order_items oi ON p.id = oi.product_id
GROUP BY rango_rating
ORDER BY revenue_total DESC;


-- 9. Cruce Complejo: Top 3 usuarios que mas gastaron + rating promedio de sus productos
SELECT
    u.name,
    ROUND(SUM(o.total_amount), 2) AS total_gastado,
    ROUND(AVG(pd.rating), 2) AS rating_promedio_productos
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN order_items oi ON o.id = oi.order_id
JOIN product_details pd ON oi.product_id = pd.product_id
GROUP BY u.id, u.name
ORDER BY total_gastado DESC
LIMIT 3;


-- 10. Metrica Definitiva: Productos con alto rating pero bajo stock (riesgo de quiebre)
-- INSIGHT: Identifica productos que son populares (alto rating + muchas ventas)
-- pero tienen stock critico. Sin la integracion de la API, este analisis
-- seria IMPOSIBLE porque no tendriamos ni el stock ni el rating.
SELECT
    p.id,
    p.name AS producto,
    c.name AS categoria,
    pd.rating,
    pd.stock AS stock_actual,
    SUM(oi.quantity) AS unidades_vendidas,
    CASE
        WHEN pd.stock < 10 THEN 'CRITICO'
        WHEN pd.stock < 30 THEN 'BAJO'
        ELSE 'OK'
    END AS alerta_stock
FROM products p
JOIN product_details pd ON p.id = pd.product_id
JOIN categories c ON p.category_id = c.id
JOIN order_items oi ON p.id = oi.product_id
WHERE pd.rating >= 4.0
GROUP BY p.id, p.name, c.name, pd.rating, pd.stock
HAVING pd.stock < 30
ORDER BY pd.stock ASC, unidades_vendidas DESC;
