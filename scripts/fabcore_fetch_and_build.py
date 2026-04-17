"""
fabcore_fetch_and_build.py
==========================
Worker principal de Fabcore Dashboard.

Flujo:
  Google Sheets  ──gspread──►  DataFrames  ──pandas──►  metrics.json

El JSON de salida contiene SOLO métricas agregadas:
  - Sin DNI, sin nombres de alumnos, sin códigos de alumno
  - Solo conteos, totales, agrupaciones por mes / nodo / carrera
  - La tabla de docentes incluye nombre/apellido (info institucional pública)

Requisitos:
    pip install gspread google-auth pandas

Variables de entorno:
    GOOGLE_CREDENTIALS   JSON de service account (string completo)
    SPREADSHEET_ID       ID del Google Sheet (opcional, tiene default)

Uso local:
    export GOOGLE_CREDENTIALS=$(cat credentials.json)
    python fabcore_fetch_and_build.py

Uso en GitHub Actions: ver fabcore-dashboard.yml
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ─── Configuración ───────────────────────────────────────────────────────────
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = os.environ.get(
    "SPREADSHEET_ID",
    "1fSygIV3AmxzHOil6b-PgZ5_LO73nyrM87YrtL02rRuo"
)
OUTPUT_PATH    = Path(os.environ.get("OUTPUT_PATH", "docs/data/metrics.json"))

# Hojas que siempre se necesitan (independiente del flag INCLUIR_DASHBOARD)
REQUIRED_SHEETS = [
    "Registro de Uso",
    "REGISTRO DE CAPACITACION",
    "Usuarios",
    "Docentes",
    "CONFIGURACION",
]

STAFF_NODO = {
    "Harold La Chira":   "Fab1-Aditiva",
    "Diego Quiroz":      "Fab1-Aditiva",
    "Dario Aylas":       "Fab1-Aditiva",
    "Mariela Elgegren":  "Fab2-Bioimpresión",
    "Joaquin Martinez":  "Fab2-Bioimpresión",
    "Sandra Mozombite":  "Fab3-Digital",
    "Sofia Franco":      "Fab3-Digital",
    "Ernesto Castro":    "Fab4-Construcción"
}

MESES_ES = {
    1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
    5:"Mayo",  6:"Junio",   7:"Julio", 8:"Agosto",
    9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre",
}

MESES_ORDER = list(MESES_ES.values())
NODOS       = ["Fab1-Aditiva", "Fab2-Bioimpresión", "Fab3-Digital", "Fab4-Construcción"]


# ─── 1. Autenticación y lectura desde Google Sheets ─────────────────────────

def get_spreadsheet() -> gspread.Spreadsheet:
    #raw = os.environ.get("GOOGLE_CREDENTIALS")
    #if not raw:
    #    sys.exit("ERROR: Variable de entorno GOOGLE_CREDENTIALS no definida.")
    #creds_dict = json.loads(raw)
    #creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    CREDS_FILE = 'credentials.json'
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def load_sheets(spreadsheet: gspread.Spreadsheet) -> dict[str, pd.DataFrame]:
    """Lee las hojas requeridas y retorna DataFrames."""
    available = {ws.title: ws for ws in spreadsheet.worksheets()}
    sheets = {}
    for name in REQUIRED_SHEETS:
        if name not in available:
            print(f"  Hoja '{name}' no encontrada — se omite.")
            continue
        records = available[name].get_all_records()
        sheets[name] = pd.DataFrame(records) if records else pd.DataFrame()
        print(f"  {name:40s}  ({len(sheets[name])} filas)")
    return sheets


# ─── 2. Enriquecimiento de DataFrames ────────────────────────────────────────

def enrich_uso(uso: pd.DataFrame, usuarios: pd.DataFrame) -> pd.DataFrame:
    uso = uso.copy()
    uso["Timestamp"] = pd.to_datetime(uso["Timestamp"], dayfirst=True, errors="coerce")
    uso = uso.dropna(subset=["Timestamp"])
    uso["Mes"]      = uso["Timestamp"].dt.month
    uso["MesNombre"]= uso["Mes"].map(MESES_ES)
    uso["Año"]      = uso["Timestamp"].dt.year
    uso["Nodo"]     = uso["FabCore Staff"].map(STAFF_NODO).fillna("Sin asignar")

    # Join carrera y tipo desde Usuarios
    u = usuarios[["Codigo", "Carrera", "Tipo de Usuario"]].copy()
    u["Codigo"] = u["Codigo"].astype(str).str.strip()
    uso["Codigo"] = uso["Codigo"].astype(str).str.strip()
    uso = uso.merge(u, on="Codigo", how="left")
    uso["Carrera"]         = uso["Carrera"].fillna("Sin carrera")
    uso["Tipo de Usuario"] = uso["Tipo de Usuario"].fillna("Desconocido")
    return uso


def enrich_cap(cap: pd.DataFrame, usuarios: pd.DataFrame) -> pd.DataFrame:
    cap = cap.copy()
    cap["Timestamp"] = pd.to_datetime(cap["Timestamp"], dayfirst=True, errors="coerce")
    cap = cap.dropna(subset=["Timestamp"])
    cap["Mes"]       = cap["Timestamp"].dt.month
    cap["MesNombre"] = cap["Mes"].map(MESES_ES)

    u = usuarios[["Codigo", "Carrera"]].copy()
    u["Codigo"] = u["Codigo"].astype(str).str.strip()
    cap["CODIGO"] = cap["CODIGO"].astype(str).str.strip()
    cap = cap.merge(u, left_on="CODIGO", right_on="Codigo", how="left")
    cap["Carrera"] = cap["Carrera"].fillna("Sin carrera")
    return cap


# ─── 3. Cálculo de métricas (sin datos personales) ──────────────────────────

def safe_dict(series: pd.Series) -> dict:
    """Convierte una Series de conteos a dict serializable."""
    return {str(k): int(v) for k, v in series.items()}


def compute_metrics(sheets: dict) -> dict:
    uso      = enrich_uso(sheets["Registro de Uso"], sheets["Usuarios"])
    cap      = enrich_cap(sheets["REGISTRO DE CAPACITACION"], sheets["Usuarios"])
    docentes = sheets["Docentes"]
    usuarios = sheets["Usuarios"]

    # Filtrar solo alumnos (excluye docentes y predocentes del conteo de atenciones)
    es_alumno = uso["Tipo de Usuario"].str.contains(
        "ESTUDIANTE|PREGRADO|MAESTRIA|DOCTORADO", na=False, case=False
    )
    alumnos_uso = uso[es_alumno]

    metrics = {}

    # ── Alumnos asistidos ────────────────────────────────────────────────────
    metrics["alumnos_asistidos"] = {
        "total"          : int(alumnos_uso["Codigo"].nunique()),
        "por_mes"        : safe_dict(alumnos_uso.groupby("MesNombre")["Codigo"].nunique()),
        "por_nodo"       : safe_dict(alumnos_uso.groupby("Nodo")["Codigo"].nunique()),
        "por_carrera"    : safe_dict(
            alumnos_uso.groupby("Carrera")["Codigo"].nunique().sort_values(ascending=False)
        ),
        # Lista de {MesNombre, Nodo, alumnos} — para gráfico de barras agrupado
        "por_mes_nodo"   : (
            alumnos_uso.groupby(["MesNombre", "Nodo"])["Codigo"]
            .nunique().reset_index().rename(columns={"Codigo":"alumnos"})
            .to_dict(orient="records")
        ),
        "por_mes_carrera": (
            alumnos_uso.groupby(["MesNombre", "Carrera"])["Codigo"]
            .nunique().reset_index().rename(columns={"Codigo":"alumnos"})
            .to_dict(orient="records")
        ),
    }

    # ── Atenciones (registros totales) ───────────────────────────────────────
    metrics["atenciones"] = {
        "total"            : int(len(uso)),
        "por_mes"          : safe_dict(uso.groupby("MesNombre").size()),
        "por_nodo"         : safe_dict(uso.groupby("Nodo").size()),
        "por_servicio"     : safe_dict(uso.groupby("Servicio").size()),
        "por_tipo_servicio": safe_dict(uso.groupby("Tipo de Servicio").size()),
        "por_mes_nodo"     : (
            uso.groupby(["MesNombre", "Nodo"]).size()
            .reset_index(name="atenciones").to_dict(orient="records")
        ),
    }

    # ── Docentes apoyados ────────────────────────────────────────────────────
    # Los docentes son personal institucional; nombre/cargo es info pública
    metrics["docentes_apoyados"] = {
        "total"      : int(docentes["Codigo"].nunique()),
        "por_nodo"   : safe_dict(docentes.groupby("Nodo")["Codigo"].nunique()),
        "por_carrera": safe_dict(
            docentes.groupby("Carrera")["Codigo"].nunique().sort_values(ascending=False)
        ),
        # Detalle sin código ni DNI
        "detalle": (
            docentes[["Nombre", "Apellido", "Carrera", "Curso", "Nodo", "Apoyo"]]
            .fillna("").to_dict(orient="records")
        ),
    }

    # ── Capacitaciones ───────────────────────────────────────────────────────
    metrics["capacitaciones"] = {
        "total_registros"       : int(len(cap)),
        "alumnos_unicos"        : int(cap["CODIGO"].nunique()),
        "por_mes"               : safe_dict(cap.groupby("MesNombre").size()),
        "alumnos_unicos_por_mes": safe_dict(cap.groupby("MesNombre")["CODIGO"].nunique()),
        "por_capacitacion"      : safe_dict(
            cap.groupby("CAPACITACION").size().sort_values(ascending=False)
        ),
        "por_carrera"           : safe_dict(
            cap.groupby("Carrera")["CODIGO"].nunique().sort_values(ascending=False)
        ),
    }

    # ── Tiempo de uso ────────────────────────────────────────────────────────
    uso["Tiempo de Uso"] = pd.to_numeric(uso["Tiempo de Uso"], errors="coerce")
    metrics["tiempo_uso_minutos"] = {
        "total"       : int(uso["Tiempo de Uso"].sum(skipna=True)),
        "por_nodo"    : {k: int(v) for k, v in uso.groupby("Nodo")["Tiempo de Uso"].sum().items()},
        "por_mes"     : {str(k): int(v) for k, v in uso.groupby("MesNombre")["Tiempo de Uso"].sum().items()},
        "por_servicio": {k: int(v) for k, v in uso.groupby("Servicio")["Tiempo de Uso"].sum().items()},
    }

    # ── Material empleado ────────────────────────────────────────────────────
    uso["Material empleado (g)"] = pd.to_numeric(uso["Material empleado (g)"], errors="coerce")
    metrics["material_gramos"] = {
        "total"   : float(round(uso["Material empleado (g)"].sum(skipna=True), 2)),
        "por_nodo": {k: float(round(v, 2)) for k, v in uso.groupby("Nodo")["Material empleado (g)"].sum().items()},
        "por_mes" : {str(k): float(round(v, 2)) for k, v in uso.groupby("MesNombre")["Material empleado (g)"].sum().items()},
    }

    # ── Resumen general ──────────────────────────────────────────────────────
    metrics["resumen"] = {
        "total_usuarios_registrados": int(len(usuarios)),
        "total_alumnos_pregrado"    : int((usuarios["Tipo de Usuario"] == "ESTUDIANTE PREGRADO").sum()),
        "total_alumnos_maestria"    : int((usuarios["Tipo de Usuario"] == "ESTUDIANTE MAESTRIA").sum()),
        "total_docentes_registrados": int((usuarios["Tipo de Usuario"] == "DOCENTE").sum()),
        "meses_con_actividad"       : sorted(uso["MesNombre"].dropna().unique().tolist(),
                                             key=lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99),
        "nodos"                     : NODOS,
        "ultima_actualizacion"      : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    return metrics


# ─── 4. Main ─────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'─'*50}")
    print("  Fabcore Dashboard — fetch & build")
    print(f"{'─'*50}")
    print(f"  Spreadsheet: {SPREADSHEET_ID}")
    print(f"  Output:      {OUTPUT_PATH}\n")

    print("[1/3] Conectando a Google Sheets...")
    spreadsheet = get_spreadsheet()
    print(f"      '{spreadsheet.title}'\n")

    print("[2/3] Leyendo hojas...")
    sheets = load_sheets(spreadsheet)

    print("\n[3/3] Calculando métricas...")
    metrics = compute_metrics(sheets)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    r = metrics["resumen"]
    print(f"\n{'─'*50}")
    print(f"  Alumnos asistidos (únicos) : {metrics['alumnos_asistidos']['total']}")
    print(f"  Docentes apoyados          : {metrics['docentes_apoyados']['total']}")
    print(f"  Alumnos capacitados        : {metrics['capacitaciones']['alumnos_unicos']}")
    print(f"  Total atenciones           : {metrics['atenciones']['total']}")
    print(f"  Usuarios registrados       : {r['total_usuarios_registrados']}")
    print(f"  Guardado en               : {OUTPUT_PATH}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()
