# ADR-001: Ordenamiento Determinista

## Estado
 Aceptado

## Contexto

A diferencia de las herramientas que generan un archivo TAR y lo guardan en disco, `tartape` es un motor de **generación en streaming**. Esto significa que el archivo TAR no existe físicamente en ningún lugar; se construye byte a byte en memoria mientras se transmite. 

En este escenario, el determinismo es la única garantía de consistencia. Si una transmisión se interrumpe (por ejemplo, al subir el volumen 10 de un total de 50) y necesitamos reanudarla, el motor debe ser capaz de **regenerar exactamente la misma secuencia de bytes** que produjo en el primer intento. 

Si el orden de los archivos no es determinista (debido a que el sistema operativo devuelve los archivos en órdenes aleatorios), la "cinta virtual" que genera `tartape` cambia su estructura en cada ejecución. Esto provoca:
1.  **Inconsistencia de Offsets:** El byte `X` donde falló la subida ya no corresponde al mismo fragmento de información, invalidando cualquier intento de reanudación.
2.  **Corrupción de Volúmenes:** Los fragmentos ya subidos no encajarán con los nuevos, generando un archivo final corrupto.
3.  **Variación de Hash:** El identificador del archivo (Hash) cambiará en cada intento aunque los datos de origen sean los mismos, haciendo imposible verificar la integridad del stream total.

## Decisión
`tartape` implementará el determinismo como un requisito estructural para garantizar que un stream sea **reproducible y reanudable**:

1.  **Garantía de Secuencia:** El motor asegurará que el orden de procesamiento de los archivos sea idéntico en cada ejecución. Por defecto, esto se logrará mediante un ordenamiento alfabético en el descubrimiento de carpetas (`add_folder`).
2.  **Persistencia del Estado T0:** El motor permitirá trabajar con una lista de archivos "congelada" (inyectada externamente). Esto permite que, si un proceso de streaming debe ser reiniciado días después, el motor utilice la misma secuencia exacta definida en el inventario inicial, independientemente de cómo el sistema operativo vea los archivos en ese momento.

## Consecuencias

*   **Positivas:**
    *  **Reanudación de Streams:** Permite retomar transmisiones interrumpidas con precisión de byte, ya que el motor puede reconstruir la "cinta" exactamente igual a como estaba.
    *  **Consistencia de Volúmenes:** Garantiza que los trozos de un archivo (volúmenes) sean matemáticamente consistentes entre sí al final del proceso.
    *  **Verificabilidad:** El Hash del flujo resultante será siempre el mismo para un conjunto de archivos dado, permitiendo auditorías de integridad fiables.

*   **Negativas:**
    *  **Sobrecarga de Inventario:** Para garantizar el orden, el motor debe conocer o listar los archivos antes de emitir el primer byte, lo que introduce una pequeña latencia inicial frente a un streaming puramente aleatorio.
    *  **Uso de Memoria en Escaneo:** El ordenamiento de estructuras de archivos masivas requiere almacenar temporalmente los nombres en memoria antes de comenzar la generación.
