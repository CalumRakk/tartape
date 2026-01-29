# TarTape

TarTape es un motor de generación de archivos TAR diseñado con un enfoque en **streaming** y en el control explícito del proceso de archivado.

Está pensado para entornos donde generar el archivo TAR completo en memoria o mediante archivos temporales en disco no es viable o deseable, como pipelines de datos, servicios web o sistemas con recursos limitados.

No intenta reemplazar todas las funcionalidades de las herramientas TAR tradicionales, sino ofrecer una alternativa clara y predecible para escenarios donde la observabilidad del flujo y la integridad de los datos son importantes.

---

## Características principales

*   **Streaming real de datos**  
    Genera un flujo continuo de bytes, facilitando su integración en pipelines donde el archivo final no necesita existir completo en disco o memoria (como subidas directas a la nube).

*   **Eficiencia de memoria**  
    El consumo de RAM es bajo y constante, independientemente del tamaño total del archivo. Esto permite procesar volúmenes masivos de datos de forma predecible.

*   **Observabilidad**  
    A diferencia de una "caja negra", TarTape emite eventos sobre el progreso. Puedes monitorear qué archivo se está procesando y reaccionar en tiempo real.

*   **Integridad ante todo**  
    El motor verifica que los archivos no cambien de tamaño mientras son leídos (ej. logs activos). Si detecta una discrepancia, detiene el proceso explícitamente para evitar generar archivos corruptos en silencio.

---

## Instalación

```bash
pip install git+https://github.com/CalumRakk/tartape.git
```

## Ejemplos de uso

### Uso Básico : genera un archivo TAR

En el caso más simple, TarTape emite los bytes del archivo TAR como un stream que puede escribirse directamente a un archivo.

```python
from tartape import TarTape, TarEventType

tape = TarTape()
tape.add_folder("./mis_datos")

with open("backup.tar", "wb") as f:
    for event in tape.stream():
        if event.type == TarEventType.FILE_DATA:
            f.write(event.data)
```


### Streaming con monitoreo y control

TarTape expone el proceso de archivado mediante eventos que permiten observar qué ocurre en cada etapa del stream.

```python
from tartape import TarTape, TarEventType

tape = TarTape()
tape.add_folder("/var/log/app")

for event in tape.stream():
    if event.type == TarEventType.FILE_START:
        # Se emite antes de procesar un archivo
        print(f"Archivando: {event.entry.arc_path} ({event.entry.size} bytes)")

    elif event.type == TarEventType.FILE_DATA:
        # Bytes crudos del TAR (headers, contenido y padding)
        # Aquí podrían enviarse directamente a red o a un bucket
        pass

    elif event.type == TarEventType.FILE_END:
        # Se emite al finalizar un archivo
        # Incluye metadatos como el hash calculado durante la lectura
        print(f"Archivo completado. MD5: {event.metadata.md5sum}")

    elif event.type == TarEventType.TAPE_COMPLETED:
        print("Archivo TAR finalizado correctamente.")

```
