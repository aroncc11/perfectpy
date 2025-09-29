# CFPB ETL Flow

Este proyecto utiliza **Prefect** para implementar un flujo ETL (Extract, Transform, Load) que descarga, procesa y almacena datos de quejas de consumidores desde la API pública de la Oficina de Protección Financiera del Consumidor (CFPB).

## Descripción del flujo ETL

El flujo ETL consta de tres etapas principales:

### 1. **Extract (Extracción)**
La función `get_complaint_data` realiza una solicitud HTTP a la API de CFPB para obtener un conjunto de datos de quejas de consumidores. 
- **URL de la API**: `https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
- **Parámetros**: Se solicita un máximo de 10 registros (`size=10`).
- **Manejo de errores**: La tarea tiene configurado un mecanismo de reintentos (`retries=3`) en caso de que la solicitud falle.

### 2. **Transform (Transformación)**
La función `parse_complaint_data` procesa los datos obtenidos de la API para extraer los campos relevantes:
- `date_received`: Fecha en que se recibió la queja.
- `state`: Estado del consumidor.
- `product`: Producto financiero relacionado con la queja.
- `company`: Compañía involucrada.
- `complaint_what_happened`: Descripción de la queja.

Los datos se transforman en una lista de tuplas para facilitar su almacenamiento.

### 3. **Load (Carga)**
La función `store_complaints` almacena los datos transformados en una base de datos SQLite:
- **Base de datos**: `cfpbcomplaints.db`
- **Tabla**: `complaint`
  - `timestamp`: Fecha en que se recibió la queja.
  - `state`: Estado del consumidor.
  - `product`: Producto financiero relacionado.
  - `company`: Compañía involucrada.
  - `complaint_what_happened`: Descripción de la queja.
- Si la tabla no existe, se crea automáticamente.

## Requisitos

- Python 3.8 o superior.
- Bibliotecas necesarias:
  - `requests`
  - `sqlite3` (incluida en la biblioteca estándar de Python)
  - `prefect`

Para instalar las dependencias, ejecuta:

```bash
pip install prefect requests
```