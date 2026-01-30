# El Paradigma de TarTape 

### 1. Introducción: Streaming de Datos vs. Backup de Sistema
`tartape` no es simplemente una implementación más del estándar TAR. Aunque respeta el formato USTAR para garantizar la compatibilidad universal, su arquitectura nace de una necesidad distinta a la de las herramientas tradicionales como GNU `tar`.

Mientras que el TAR tradicional fue diseñado para el **Backup de Sistema** (preservar el estado exacto de una máquina física), `tartape` está diseñado para el **Streaming de Datos** (transportar información de forma eficiente, segura y reproducible a través de redes y nubes).

### 2. Comparativa de Enfoques

| Característica | Backup de Sistema (Tradicional) | Streaming de Datos (TarTape) |
| :--- | :--- | :--- |
| **Objetivo Primario** | Restaurar un Sistema Operativo completo. | Sincronizar y transportar datos entre nodos. |
| **Identidad** | Preserva UID/GID locales (vital para el SO). | Anonimiza Identidad (privacidad y portabilidad). |
| **Funcionalidad** | Preserva permisos (ejecución, lectura). | Preserva permisos (conveniencia del usuario). |
| **Determinismo** | El orden es irrelevante (un solo archivo final). | El orden es crítico (define offsets para reanudación). |
| **Integridad** | Basada en el momento de la lectura. | Basada en una "Foto Instantánea" (T0). |

### 3. Los Tres Pilares de TarTape

#### I. Determinismo como Contrato
En un entorno de streaming, especialmente cuando el flujo se divide en volúmenes o partes, el orden de los archivos no es una sugerencia, es un **Contrato de Secuencia**. 
Si el archivo A se procesa antes que el archivo B, esto define el byte exacto donde empieza cada uno. `tartape` garantiza que este orden sea persistente para permitir que cualquier operación de streaming sea reanudable bit a bit.

#### II. Portabilidad Consciente
`tartape` distingue entre **Identidad** y **Comportamiento**:
*   **Identidad (UID/GID):** Se considera "ruido ambiental" del sistema de origen. Se aplana (anonymize) para garantizar que el backup no filtre datos privados y sea reproducible en cualquier máquina.
*   **Comportamiento (Permisos/Mode):** Se conserva fielmente. Un archivo ejecutable en el origen debe seguir siendo ejecutable en el destino.

#### III. La "Imagen T0" (Verdad Absoluta)
Para `tartape`, el proceso de archivado se divide en dos fases:
1.  **T0 (Inventario):** Se captura la "Promesa" (Nombre, Tamaño, mtime).
2.  **T1 (Streaming):** Se ejecuta la promesa.

Si en el momento T1 el disco contradice la promesa hecha en T0 (el archivo mutó o cambió de tamaño), `tartape` aborta la operación. Esto protege la integridad de la estructura del TAR y evita que el receptor reciba datos inconsistentes.

### 4. Conclusión
`tartape` sacrifica la capacidad estandar de clonar sistemas operativos a cambio de convertirse en una herramienta quirúrgica para el transporte de datos. Es un motor diseñado para la era de la nube, donde la **observabilidad**, la **reanudación** y el **determinismo** son más importantes que la fidelidad a los IDs de usuario locales.

