# Fabcore Dashboard

Dashboard de métricas operativas de **Fabcore PUCP**, publicado en GitHub Pages y actualizado automáticamente cada día.

## Vista del dashboard

El dashboard está disponible en la URL de GitHub Pages de este repositorio.

## Estructura

```
.github/workflows/   ← Automatización
docs/                ← Sitio web público
scripts/             ← Procesamiento de datos
```

## Actualización de datos

Los datos se actualizan automáticamente todos los días a las **6:00 AM (Lima)**. También pueden actualizarse manualmente desde **Actions → Run workflow**.

## Depuración local

### Requisitos

- Anaconda
- Archivo `credentials.json` en la raíz del proyecto (acceso restringido)

### Pasos

```bash
# Crear y activar entorno
conda create -n FabcoreMetrics python=3.11 -y
conda activate FabcoreMetrics

# Instalar dependencias
pip install gspread google-auth pandas openpyxl

# Generar datos
python scripts/fabcore_fetch_and_build.py

# Previsualizar en el navegador
python -m http.server 8080 --directory docs
# → http://localhost:8080
```

> El archivo `credentials.json` es de uso interno y no debe subirse al repositorio.