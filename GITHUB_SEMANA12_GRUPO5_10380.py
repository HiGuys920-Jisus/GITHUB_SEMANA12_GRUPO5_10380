INTEGRANTES:

* Jesús Enmanuel Aguirre Ramos

* Gino Alexander Bujaico Gutiérrez

* Juan Claudio Wilhelm Llontop Aguayo

* Henry Johan Sneider Enciso Canales
"""

from pyspark import SparkContext

sc = SparkContext("local", "Comercializacion")

datos = [
    ("Camiseta Estampada", 10.0, 2),
    ("Zapatos de Cuero", 75.0, 3),
    ("Gorra de Beisbol", 15.0, 5),
    ("Reloj Deportivo", 50.0, 1),
    ("Chaqueta de Invierno", 100.0, 4),
    ("Auriculares Inalámbricos", 30.0, 2),
    ("Mochila de Senderismo", 45.0, 1),
    ("Gafas de Sol", 20.0, 7),
    ("Smartphone", 300.0, 3),
    ("Lámpara de Mesa", 25.0, 5),
]

rdd = sc.parallelize(datos)

total_ventas = rdd.map(lambda x: (x[0], x[1] * x[2])) \
                  .reduceByKey(lambda a, b: a + b)

precio_total_por_producto = total_ventas.map(lambda x: (x[0], f"Total: {x[1]:.2f}"))

total_ventas_global = total_ventas.map(lambda x: x[1]).reduce(lambda a, b: a + b)

total_productos_vendidos = rdd.map(lambda x: x[2]).reduce(lambda a, b: a + b)

promedio_ventas = total_ventas_global / total_productos_vendidos

producto_mas_vendido = rdd.map(lambda x: (x[0], x[2])) \
                           .reduceByKey(lambda a, b: a + b) \
                           .reduce(lambda a, b: a if a[1] > b[1] else b)

resultados_totales = total_ventas.collect()

print(precio_total_por_producto.collect())

# Imprime resultados de ventas totales
print("Resultados de Ventas Totales:")
for producto, total in resultados_totales:
    print(f"{producto}: ${total:.2f}")

# Imprime total de ventas global
print(f"\nTotal Ventas Global: ${total_ventas_global:.2f}")
print(f"Total Productos Vendidos: {total_productos_vendidos}")
print(f"Promedio Ventas por Producto: ${promedio_ventas:.2f}")
print(f"Producto Más Vendido: {producto_mas_vendido[0]} con {producto_mas_vendido[1]} unidades")

# Usar SparkContext.range para simular un escenario de ventas adicionales
unidades_adicionales = sc.range(1, len(datos) + 1).map(lambda x: (f"Producto {x}", x))

# Combina las unidades adicionales con las ventas originales
ventas_finales = total_ventas.zip(unidades_adicionales).map(lambda x: (x[0][0], x[0][1] + x[1][1]))

# Obtiene los resultados finales
resultados_finales = ventas_finales.collect()

# Imprime resultados finales con unidades adicionales
print("\nResultados Finales con Unidades Adicionales:")
for producto, total in resultados_finales:
    print(f"{producto}: ${total:.2f}")
