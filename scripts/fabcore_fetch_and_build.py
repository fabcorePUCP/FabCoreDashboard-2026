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
    "CURSOS PUCP",
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

# Tipos de usuario que corresponden a docentes / jefes de práctica
TIPOS_DOC_JP = ["DOCENTE", "PREDOCENTE"]
TIPOS_ALUMNO = ["ESTUDIANTE PREGRADO", "ESTUDIANTE MAESTRIA", "ESTUDIANTE DOCTORADO"]


# ─── 1. Autenticación y lectura desde Google Sheets ─────────────────────────

def get_spreadsheet() -> gspread.Spreadsheet:
    #raw = os.environ.get("GOOGLE_CREDENTIALS")
    #if not raw:
    #    sys.exit("ERROR: Variable de entorno GOOGLE_CREDENTIALS no definida.")
    #creds_dict = json.loads(raw)
    #creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    CREDS_FILE = '../credentials.json'
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
        records = available[name].get_all_records(numericise_ignore=[7,8])
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

    uso["CursoCodigo"] = (
        uso["Curso"].fillna("").str.extract(r"^([A-Z0-9]+)", expand=False).str.strip()
    )

    # Join carrera y tipo desde Usuarios
    u = usuarios[["Codigo", "Carrera", "Tipo de Usuario"]].copy()
    u["Codigo"] = u["Codigo"].astype(str).str.strip()
    uso["Codigo"] = uso["Codigo"].astype(str).str.strip()
    uso = uso.merge(u, on="Codigo", how="left")
    uso["Carrera"]         = uso["Carrera"].fillna("Sin carrera")
    uso["Tipo de Usuario"] = uso["Tipo de Usuario"].fillna("Desconocido")

    # Material: normalizar coma decimal
    uso["Material empleado (g)"] = pd.to_numeric(uso['Material empleado (g)'].str.replace(',', '.'), errors="coerce")
    uso["Tiempo de Uso"] = pd.to_numeric(uso["Tiempo de Uso"].str.replace(',', '.'), errors="coerce")
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
    cursos   = sheets["CURSOS PUCP"]

    alumnos_uso = uso[uso["Tipo de Usuario"].isin(TIPOS_ALUMNO)]
    docjp_uso   = uso[uso["Tipo de Usuario"].isin(TIPOS_DOC_JP)]

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

    # ── Atenciones a docentes y jefes de práctica ────────────────────────────
    metrics["atenciones_doc_jp"] = {
        # Conteos
        "total_registros"        : int(len(docjp_uso)),
        "docentes_unicos"        : int(docjp_uso[docjp_uso["Tipo de Usuario"]=="DOCENTE"]["Codigo"].nunique()),
        "predocentes_unicos"     : int(docjp_uso[docjp_uso["Tipo de Usuario"]=="PREDOCENTE"]["Codigo"].nunique()),
        # Totales por tipo de usuario
        "por_tipo"               : safe_dict(docjp_uso.groupby("Tipo de Usuario").size()),
        "por_mes"                : safe_dict(docjp_uso.groupby("MesNombre").size()),
        "por_nodo"               : safe_dict(docjp_uso.groupby("Nodo").size()),
        "por_servicio"           : safe_dict(docjp_uso.groupby("Servicio").size()),
        # Detalle: atenciones por mes y nodo
        "por_mes_nodo"     : (
            docjp_uso.groupby(["MesNombre", "Nodo"]).size()
            .reset_index(name="atenciones").to_dict(orient="records")
        ),
        # Detalle: atenciones por mes y tipo (para gráfico apilado)
        "por_mes_tipo"           : (
            docjp_uso.groupby(["MesNombre", "Tipo de Usuario"]).size()
            .reset_index(name="atenciones").to_dict(orient="records")
        ),
        # Registros únicos por nodo y tipo
        "unicos_por_nodo_tipo"   : (
            docjp_uso.groupby(["Nodo", "Tipo de Usuario"])["Codigo"]
            .nunique().reset_index(name="personas").to_dict(orient="records")
        ),
    }

    # ── Docentes apoyados ────────────────────────────────────────────────────
    doc = docentes.copy()
    doc["Nodo"] = doc["Nodo"].str.strip().str.replace(" ", "").str.lower()
    NODO_NORM = {
        "fabcore1": "Fab1-Aditiva",
        "fabcore2": "Fab2-Bioimpresión",
        "fabcore3": "Fab3-Digital",
        "fabcore4": "Fab4-Construcción",
    }
    doc["NodoNorm"] = doc["Nodo"].map(NODO_NORM).fillna(doc["Nodo"])

    # Los docentes son personal institucional; nombre/cargo es info pública
    metrics["docentes_vinculados"] = {
        "total"      : int(doc["Codigo"].nunique()),
        "por_nodo"   : safe_dict(doc.groupby("NodoNorm")["Codigo"].nunique()),
        "por_carrera": safe_dict(
            doc.groupby("Carrera")["Codigo"].nunique().sort_values(ascending=False)
        ),
        # Tipo de vínculo inferido del campo Apoyo
        "convenio"      : int(doc["Apoyo"].str.contains("Convenio|convenio", na=False).sum()),
        "asesor_tesis"  : int(doc["Apoyo"].str.contains("[Aa]sesor", na=False).sum()),
        "apoyo_curso"   : int(
            (~doc["Apoyo"].str.contains("Convenio|convenio|[Aa]sesor", na=False) &
              doc["Apoyo"].notna()).sum()
        ),
        # Detalle sin código ni DNI
        "detalle": (
            doc[["Nombre", "Apellido", "Carrera", "Curso", "NodoNorm", "Apoyo"]]
            .rename(columns={"NodoNorm": "Nodo"})
            .fillna("").to_dict(orient="records")
        ),
    }

    # ── Convenios de curso por nodo ──────────────────────────────────────────
    conv = cursos[cursos["CONVENIO"].astype(str).str.upper() == "SI"].copy()
    conv["NodoNorm"] = (
        conv["Nodo"].fillna("Sin asignar").str.strip().str.upper()
        .map({"FABCORE 1": "Fab1-Aditiva", "FABCORE 2": "Fab2-Bioimpresión",
              "FABCORE 3": "Fab3-Digital",  "FABCORE 4": "Fab4-Construcción"})
        .fillna("Sin asignar")
    )
    conv["FECHA INICIO DE CONVENIO"] = pd.to_datetime(
        conv["FECHA INICIO DE CONVENIO"], dayfirst=True, errors="coerce"
    ).dt.strftime("%Y-%m-%d").fillna("")

    metrics["convenios"] = {
        "total"    : int(len(conv)),
        "por_nodo" : safe_dict(conv.groupby("NodoNorm").size()),
        # Lista completa para tabla (sin datos sensibles)
        "detalle"  : (
            conv[["CODIGO", "NOMBRE", "NodoNorm", "FECHA INICIO DE CONVENIO", "Notas"]]
            .rename(columns={"NodoNorm": "Nodo", "FECHA INICIO DE CONVENIO": "fecha_convenio"})
            .fillna("").to_dict(orient="records")
        ),
    }

    # ── Material empleado por curso ──────────────────────────────────────────
    mat_uso = uso.dropna(subset=["Material empleado (g)"])
    mat_uso = mat_uso[mat_uso["Material empleado (g)"] > 0].copy()
 
    # Clasificar cada registro: Curso / Proyecto / Tesis
    def tipo_registro(cod):
        c = str(cod).upper().strip()
        if c.startswith("PROY"): return "Proyecto"
        if c.startswith("TES"):  return "Tesis"
        return "Curso"
 
    mat_uso["TipoRegistro"] = mat_uso["CursoCodigo"].apply(tipo_registro)
 
    # Normalizar material: todo lo que no es PLA ni Resina → "Otros"
    RESINAS = {"Resina 1", "Resina Estandar"}
    def norm_mat(m):
        if m == "PLA": return "PLA"
        if m in RESINAS: return "Resina"
        return "Otros"
    mat_uso["MaterialNorm"] = mat_uso["Material"].apply(norm_mat)
 
    # Nombre limpio del curso: tomar el campo Curso completo, quitar código inicial
    # Ej: "1MTR52 - PROYECTO DE DISEÑO MECATRÓNICO" → "PROYECTO DE DISEÑO MECATRÓNICO"
    mat_uso["NombreCurso"] = (
        mat_uso["Curso"].fillna("").str.replace(r"^[A-Za-z0-9]+\s*-\s*", "", regex=True).str.strip()
    )
    # Para registros con mismo código pero nombre escrito distinto, normalizar a mayúsculas
    mat_uso["NombreCurso"] = mat_uso["NombreCurso"].str.upper()
 
    # ── Resumen global de material ────────────────────────────────────────────
    mat_por_nodo  = {k: round(float(v), 2) for k, v in mat_uso.groupby("Nodo")["Material empleado (g)"].sum().items()}
    mat_por_tipo  = {k: round(float(v), 2) for k, v in mat_uso.groupby("MaterialNorm")["Material empleado (g)"].sum().items()}
    mat_por_mes   = {str(k): round(float(v), 2) for k, v in mat_uso.groupby("MesNombre")["Material empleado (g)"].sum().items()}
    mat_por_treg  = {k: round(float(v), 2) for k, v in mat_uso.groupby("TipoRegistro")["Material empleado (g)"].sum().items()}
    mat_por_mes_nodo = (uso.groupby(["MesNombre", "Nodo"])["Material empleado (g)"]
                        .sum().reset_index()
                        .rename(columns={"Material empleado (g)": "material"})
                        .to_dict(orient="records")
                        )

    # ── Helper: tabla de material por entidad (código + nombre) ───────────────
    def tabla_mat(df):
        """Devuelve lista de {codigo, nombre, PLA, Resina, Otros, total}"""
        g = (
            df.groupby(["CursoCodigo", "NombreCurso", "MaterialNorm"])["Material empleado (g)"]
            .sum().reset_index()
        )
        pivot = g.pivot_table(
            index=["CursoCodigo", "NombreCurso"],
            columns="MaterialNorm", values="Material empleado (g)",
            aggfunc="sum", fill_value=0
        ).reset_index()
        pivot.columns.name = None
        for col in ["PLA", "Resina", "Otros"]:
            if col not in pivot.columns:
                pivot[col] = 0.0
        pivot["total"] = pivot[["PLA", "Resina", "Otros"]].sum(axis=1)
        pivot = pivot.sort_values("total", ascending=False)
        return [
            {
                "codigo":  row["CursoCodigo"],
                "nombre":  row["NombreCurso"],
                "PLA":     round(float(row["PLA"]),   2),
                "Resina":  round(float(row["Resina"]), 2),
                "Otros":   round(float(row["Otros"]),  2),
                "total":   round(float(row["total"]),  2),
            }
            for _, row in pivot.iterrows()
        ]
 
    # Subconjuntos por tipo
    df_cursos    = mat_uso[mat_uso["TipoRegistro"] == "Curso"]
    df_proyectos = mat_uso[mat_uso["TipoRegistro"] == "Proyecto"]
    df_tesis     = mat_uso[mat_uso["TipoRegistro"] == "Tesis"]
 
    metrics["material"] = {
        # Totales globales
        "total_gramos"  : round(float(mat_uso["Material empleado (g)"].sum()), 2),
        "por_tipo"      : mat_por_tipo,       # {PLA, Resina, Otros}
        "por_nodo"      : mat_por_nodo,
        "por_mes"       : mat_por_mes,
        "por_mes_nodo"  : mat_por_mes_nodo,
        "por_tipo_registro": mat_por_treg,    # {Curso, Proyecto, Tesis}
        # Tablas desglosadas por tipo de registro
        "cursos"        : tabla_mat(df_cursos),
        "proyectos"     : tabla_mat(df_proyectos),
        "tesis"         : tabla_mat(df_tesis),
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

    # ── Equipos más usados ───────────────────────────────────────────────────
    # Normalizar nombres (case + trim) antes de agrupar
    uso["EquipoNorm"] = uso["Equipo Empleado"].str.upper().str.strip()
    # Unificar variantes del mismo equipo
    EQUIPO_ALIAS = {"BIOIMPRESORA": "BIOIMPRESORA TISSUESTART"}
    uso["EquipoNorm"] = uso["EquipoNorm"].replace(EQUIPO_ALIAS)
 
    eq_grp = uso[uso["EquipoNorm"].notna() & (uso["EquipoNorm"] != "")].groupby("EquipoNorm")
    eq_atenciones = eq_grp.size().sort_values(ascending=False)
    eq_tiempo     = eq_grp["Tiempo de Uso"].sum().reindex(eq_atenciones.index).fillna(0)
    eq_material   = eq_grp["Material empleado (g)"].sum().reindex(eq_atenciones.index).fillna(0)
 
    metrics["equipos"] = {
        "ranking": [
            {
                "equipo"    : equipo,
                "atenciones": int(eq_atenciones[equipo]),
                "tiempo_min": int(eq_tiempo[equipo]),
                "material_g": round(float(eq_material[equipo]), 2),
            }
            for equipo in eq_atenciones.index
            if equipo not in {"USO DE ESPACIO (NO EQUIPO)"}
        ]
    }
 
    # ── Tasa de retorno ──────────────────────────────────────────────────────
    alumnos_uso_ret = uso[uso["Tipo de Usuario"].isin(TIPOS_ALUMNO)]
    visitas_alumno  = alumnos_uso_ret.groupby("Codigo").size()
    total_alumnos   = len(visitas_alumno)
 
    # Distribución de frecuencia: {1: N, 2: N, 3+: N, 5+: N …}
    freq_dist = visitas_alumno.value_counts().sort_index()
    # Agrupar todo lo que sea ≥ 5 en un solo bucket "5+"
    freq_agrupada = {}
    for n_visitas, count in freq_dist.items():
        key = str(n_visitas) if n_visitas < 5 else "5+"
        freq_agrupada[key] = freq_agrupada.get(key, 0) + int(count)
 
    # Retorno por nodo
    alumnos_uso_ret2 = alumnos_uso_ret.copy()
    retorno_por_nodo = {}
    for nodo, grp in alumnos_uso_ret2.groupby("Nodo"):
        v = grp.groupby("Codigo").size()
        recurrentes = int((v > 1).sum())
        total_n     = len(v)
        retorno_por_nodo[nodo] = {
            "total"       : total_n,
            "recurrentes" : recurrentes,
            "tasa_pct"    : round(recurrentes / total_n * 100, 1) if total_n else 0,
        }
 
    recurrentes_global = int((visitas_alumno > 1).sum())
    metrics["retorno"] = {
        "total_alumnos"    : total_alumnos,
        "unicos"           : int((visitas_alumno == 1).sum()),
        "recurrentes"      : recurrentes_global,
        "tasa_global_pct"  : round(recurrentes_global / total_alumnos * 100, 1) if total_alumnos else 0,
        "frecuencia_dist"  : freq_agrupada,   # {1: N, 2: N, 3: N, 4: N, "5+": N}
        "por_nodo"         : retorno_por_nodo,
        "max_visitas"      : int(visitas_alumno.max()),
    }
 
    # ── Heatmap horario (día × hora) ─────────────────────────────────────────
    DIAS_ES = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves", 4: "Viernes",
               5: "Sábado", 6: "Domingo"}
    uso["DiaSemana"] = uso["Timestamp"].dt.dayofweek
    uso["Hora"]      = uso["Timestamp"].dt.hour
 
    heatmap_raw = (
        uso.groupby(["DiaSemana", "Hora"])
        .size()
        .reset_index(name="atenciones")
    )
 
    # Construir matriz 5×(horas con datos) como lista de {dia, hora, valor}
    heatmap_cells = [
        {
            "dia"        : DIAS_ES[int(row["DiaSemana"])],
            "dia_idx"    : int(row["DiaSemana"]),  # 0-4 para ordenar
            "hora"       : int(row["Hora"]),
            "atenciones" : int(row["atenciones"]),
        }
        for _, row in heatmap_raw.iterrows()
        if int(row["DiaSemana"]) < 5  # solo Lun-Vie
    ]
 
    metrics["heatmap_horario"] = {
        "celdas"    : heatmap_cells,
        "max_valor" : int(heatmap_raw["atenciones"].max()),
        "dias"      : ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes"],
        "horas"     : sorted(uso["Hora"].dropna().unique().astype(int).tolist()),
    }

    # ── Tiempo de uso ────────────────────────────────────────────────────────
    uso["Tiempo de Uso"] = pd.to_numeric(uso["Tiempo de Uso"], errors="coerce")
    metrics["tiempo_uso_minutos"] = {
        "total"       : int(uso["Tiempo de Uso"].sum(skipna=True)),
        "por_nodo"    : {k: int(v) for k, v in uso.groupby("Nodo")["Tiempo de Uso"].sum().items()},
        "por_mes"     : {str(k): int(v) for k, v in uso.groupby("MesNombre")["Tiempo de Uso"].sum().items()},
        "por_servicio": {k: int(v) for k, v in uso.groupby("Servicio")["Tiempo de Uso"].sum().items()},
        "por_mes_nodo" : (
            uso.groupby(["MesNombre", "Nodo"])["Tiempo de Uso"]
            .sum().reset_index()
            .rename(columns={"Tiempo de Uso": "minutos"})
            .to_dict(orient="records")
        ),
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
    print(f"  Docentes/JP atendidos      : {metrics['atenciones_doc_jp']['docentes_unicos']}D / {metrics['atenciones_doc_jp']['predocentes_unicos']}JP")
    print(f"  Docentes vinculados        : {metrics['docentes_vinculados']['total']}")
    print(f"  Alumnos capacitados        : {metrics['capacitaciones']['alumnos_unicos']}")
    print(f"  Total atenciones           : {metrics['atenciones']['total']}")
    print(f"  Usuarios registrados       : {r['total_usuarios_registrados']}")
    print(f"  Convenios establecidos     : {metrics['convenios']['total']}")
    print(f"  Material total (g)         : {metrics['material']['total_gramos']}")
    print(f"  Guardado en               : {OUTPUT_PATH}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()
