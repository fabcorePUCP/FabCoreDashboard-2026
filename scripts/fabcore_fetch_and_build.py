"""
fabcore_fetch_and_build.py
==========================
Worker principal de Fabcore Dashboard.

Flujo:
  Google Sheets  ──gspread──►  DataFrames  ──pandas──►  metrics.json

El JSON de salida contiene registros a nivel de fila (sin PII), para que
el dashboard pueda filtrar por Nodo y Mes y recalcular TODAS las métricas
(no solo los KPIs) en el cliente.

Privacidad:
  - Sin DNI, sin nombres de alumnos, sin códigos de alumno en texto plano.
  - "codigo_hash" es un hash corto no reversible (sha1 truncado) usado solo
    para contar alumnos únicos / recurrencia.
  - La tabla de docentes incluye nombre/apellido (info institucional pública).

Requisitos:
    pip install gspread google-auth pandas

Variables de entorno:
    GOOGLE_CREDENTIALS   JSON de service account (string completo)
    SPREADSHEET_ID       ID del Google Sheet (opcional, tiene default)
    HASH_SALT            sal para el hash de códigos (opcional, recomendado)

Uso local:
    export GOOGLE_CREDENTIALS=$(cat credentials.json)
    python fabcore_fetch_and_build.py

Uso en GitHub Actions: ver fabcore-dashboard.yml
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone, date
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
HASH_SALT      = os.environ.get("HASH_SALT", "fabcore-2026")

# Hojas que siempre se necesitan
REQUIRED_SHEETS = [
    "Registro de Uso",
    "REGISTRO DE CAPACITACION",
    "Usuarios",
    "Docentes",
    "CURSOS PUCP",
    "CONFIGURACION",
]

# Cada entrada: (nombre_staff, nodo, fecha_desde)
# Las reglas se evalúan en orden; gana la más reciente cuya fecha <= fecha de la atención.
# "fecha_desde=None" significa "desde siempre" (regla base).
STAFF_NODO_HISTORY = [
    # Reglas base (sin fecha límite inferior)
    ("Harold La Chira",   "Fab1-Aditiva",        None),
    ("Diego Quiroz",      "Fab1-Aditiva",         None),
    ("Dario Aylas",       "Fab1-Aditiva",         None),
    ("Jefferson Castañeda", "Fab1-Aditiva",         None),
    ("Mariela Elgegren",  "Fab2-Bioimpresión",    None),
    ("Joaquin Martinez",  "Fab2-Bioimpresión",    None),
    ("Brenda Cárdenas",   "Fab2-Bioimpresión",    None),
    ("Sandra Mozombite",  "Fab3-Digital",          None),
    ("Sofia Franco",      "Fab3-Digital",          None),
    ("Ernesto Castro",    "Fab3-Digital",          None),
    ("Ernesto Castro",    "Fab3-Digital",          "2026-07-10"),
    # Joaquin Dulanto: Fab3 hasta el 14 de junio, Fab1 desde el 15
    ("Joaquin Dulanto",   "Fab3-Digital",          None),
    ("Joaquin Dulanto",   "Fab1-Aditiva",          "2026-06-15"),
    ("Tayel Saavedra",    "Fab1-Digital",          None),
    ("Wilder Céspedes",   "Fab3-Digital",          None),
]

# Equipos de concreto: cualquier atención que use estos equipos se clasifica
# como Fab4-Construcción y su material se interpreta en KILOGRAMOS
# (se convierte a gramos ×1000 para mantener unidades consistentes en el JSON).
EQUIPOS_CONCRETO = {"COLIBRÍ 1", "COLIBRÍ 2", "COLIBRÍ INDUSTRIAL", "BRAZO YASKAWA", "MESA DE FLUJO", "BOMBA MAI"}

MESES_ES = {
    1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril",
    5:"Mayo",  6:"Junio",   7:"Julio", 8:"Agosto",
    9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre",
}
DIAS_ES = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves", 4: "Viernes",
           5: "Sábado", 6: "Domingo"}

MESES_ORDER = list(MESES_ES.values())
NODOS       = ["Fab1-Aditiva", "Fab2-Bioimpresión", "Fab3-Digital", "Fab4-Construcción"]

# Tipos de usuario que corresponden a docentes / jefes de práctica
TIPOS_DOC_JP = ["DOCENTE", "PREDOCENTE"]
TIPOS_ALUMNO = ["ESTUDIANTE PREGRADO", "ESTUDIANTE MAESTRIA", "ESTUDIANTE DOCTORADO"]

RESINAS = {"Resina 1", "Resina Estandar"}

NODO_CURSO_MAP = {
    "FABCORE 1": "Fab1-Aditiva", "FABCORE 2": "Fab2-Bioimpresión",
    "FABCORE 3": "Fab3-Digital",  "FABCORE 4": "Fab4-Construcción",
}

NODO_DOC_MAP = {
    "fabcore1": "Fab1-Aditiva",
    "fabcore2": "Fab2-Bioimpresión",
    "fabcore3": "Fab3-Digital",
    "fabcore4": "Fab4-Construcción",
}


# ─── 1. Autenticación y lectura desde Google Sheets ─────────────────────────

def get_spreadsheet() -> gspread.Spreadsheet:
    raw = os.environ.get("GOOGLE_CREDENTIALS")
    if raw:
        try:
            creds_dict = json.loads(raw)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        except json.JSONDecodeError:
            sys.exit("ERROR: GOOGLE_CREDENTIALS no es un JSON válido.")
    else:
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
        records = available[name].get_all_records(numericise_ignore=[7, 8])
        sheets[name] = pd.DataFrame(records) if records else pd.DataFrame()
        print(f"  {name:40s}  ({len(sheets[name])} filas)")
    return sheets


# ─── 2. Helpers ──────────────────────────────────────────────────────────────

def hash_codigo(codigo) -> str:
    """Hash corto y no reversible de un código de alumno/docente."""
    s = f"{HASH_SALT}:{str(codigo).strip()}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]


def norm_material(m) -> str:
    if m == "PLA":
        return "PLA"
    if m in RESINAS:
        return "Resina"
    return "Otros"


def tipo_registro_from_codigo(cod) -> str:
    c = str(cod).upper().strip()
    if c.startswith("PROY"):
        return "Proyecto"
    if c.startswith("TES"):
        return "Tesis"
    return "Curso"


def safe_str(v) -> str:
    """Convierte NaN/None a cadena vacía; el resto a str."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return str(v)


def get_nodo_for_staff(nombre: str, fecha: date) -> str:
    """Devuelve el nodo vigente para un staff en una fecha dada."""
    resultado = None
    fecha_resultado = None
    for staff, nodo, desde in STAFF_NODO_HISTORY:
        if staff != nombre:
            continue
        desde_date = date.fromisoformat(desde) if desde else date.min
        if desde_date <= fecha:
            # Es una regla aplicable; quedarse con la más reciente
            if fecha_resultado is None or desde_date > fecha_resultado:
                resultado = nodo
                fecha_resultado = desde_date
    return resultado or "Sin asignar"

# ─── 3. Enriquecimiento de DataFrames ────────────────────────────────────────

def enrich_uso(uso: pd.DataFrame, usuarios: pd.DataFrame) -> pd.DataFrame:
    uso = uso.copy()
    uso["Timestamp"] = pd.to_datetime(uso["Timestamp"], dayfirst=True, errors="coerce")
    uso = uso.dropna(subset=["Timestamp"])

    uso["Mes"]       = uso["Timestamp"].dt.month
    uso["MesNombre"] = uso["Mes"].map(MESES_ES)
    uso["DiaSemana"] = uso["Timestamp"].dt.dayofweek
    uso["Hora"]      = uso["Timestamp"].dt.hour

    # Equipo normalizado (mayúsculas + trim + alias)
    uso["EquipoNorm"] = uso["Equipo Empleado"].astype(str).str.upper().str.strip()
    EQUIPO_ALIAS = {"BIOIMPRESORA": "BIOIMPRESORA TISSUESTART"}
    uso["EquipoNorm"] = uso["EquipoNorm"].replace(EQUIPO_ALIAS)

    # Concreto → Fab4, sin importar el staff asignado
    uso["EsConcreto"] = uso["EquipoNorm"].isin(EQUIPOS_CONCRETO)

    uso["Nodo"] = uso.apply(
        lambda r: get_nodo_for_staff(r["FabCore Staff"], r["Timestamp"].date()),
        axis=1
    )
    uso.loc[uso["EsConcreto"], "Nodo"] = "Fab4-Construcción"

    uso["CursoCodigo"] = (
        uso["Curso"].fillna("").str.extract(r"^([A-Z0-9]+)", expand=False).str.strip()
    )
    uso["TipoRegistro"] = uso["CursoCodigo"].apply(tipo_registro_from_codigo)

    # Normalizar servicio (unificar variantes de mayúsculas/minúsculas)
    uso["Servicio"] = uso["Servicio"].astype(str).str.strip()
    SERVICIO_ALIAS = {"OTROS": "Otros"}
    uso["Servicio"] = uso["Servicio"].replace(SERVICIO_ALIAS)

    uso["NombreCurso"] = (
        uso["Curso"].fillna("")
        .str.replace(r"^[A-Za-z0-9]+\s*-\s*", "", regex=True)
        .str.strip().str.upper()
    )

    # Join carrera y tipo desde Usuarios
    u = usuarios[["Codigo", "Carrera", "Tipo de Usuario"]].copy()
    u["Codigo"] = u["Codigo"].astype(str).str.strip()
    uso["Codigo"] = uso["Codigo"].astype(str).str.strip()
    uso = uso.merge(u, on="Codigo", how="left")
    uso["Carrera"]         = uso["Carrera"].fillna("Sin carrera")
    uso["Tipo de Usuario"] = uso["Tipo de Usuario"].fillna("Desconocido")

    # Hash del código (no exponer código real)
    uso["CodigoHash"] = uso["Codigo"].apply(hash_codigo)

    # Material: normalizar coma decimal → gramos
    uso["Material empleado (g)"] = pd.to_numeric(
        uso["Material empleado (g)"].astype(str).str.replace(",", "."), errors="coerce"
    ).fillna(0.0)

    # Para filas de concreto (Fab4), el valor registrado está en KG → convertir a g
    uso.loc[uso["EsConcreto"], "Material empleado (g)"] = (
        uso.loc[uso["EsConcreto"], "Material empleado (g)"] * 1000
    )

    uso["MaterialNorm"] = uso["Material"].apply(norm_material)

    uso["Tiempo de Uso"] = pd.to_numeric(
        uso["Tiempo de Uso"].astype(str).str.replace(",", "."), errors="coerce"
    ).fillna(0.0)

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

    cap["CodigoHash"] = cap["CODIGO"].apply(hash_codigo)

    # Nodo: normalizar "FABCORE 1" -> "Fab1-Aditiva", etc.
    cap["Nodo"] = (
        cap["Nodo"].fillna("Sin asignar").astype(str).str.strip().str.upper()
        .map(NODO_CURSO_MAP).fillna("Sin asignar")
    )

    return cap


# ─── 4. Construcción de registros (record-level, sin agregaciones fijas) ────

def build_atenciones(uso: pd.DataFrame) -> list[dict]:
    eq_excluir = {"USO DE ESPACIO (NO EQUIPO)"}
    rows = []
    for _, r in uso.iterrows():
        rows.append({
            "fecha"        : r["Timestamp"].strftime("%Y-%m-%d"),
            "mes"          : r["MesNombre"],
            "anio"         : int(r["Timestamp"].year),
            "dia_idx"      : int(r["DiaSemana"]),
            "dia"          : DIAS_ES[int(r["DiaSemana"])],
            "hora"         : int(r["Hora"]),
            "nodo"         : r["Nodo"],
            "carrera"      : r["Carrera"],
            "tipo_usuario" : r["Tipo de Usuario"],
            "codigo_hash"  : r["CodigoHash"],
            "curso_codigo" : r["CursoCodigo"],
            "nombre_curso" : r["NombreCurso"],
            "tipo_registro": r["TipoRegistro"],
            "servicio"     : r["Servicio"],
            "tipo_servicio": r["Tipo de Servicio"],
            "equipo"       : r["EquipoNorm"] if r["EquipoNorm"] not in eq_excluir else None,
            "tiempo_min"   : round(float(r["Tiempo de Uso"]), 2),
            "material_g"   : round(float(r["Material empleado (g)"]), 2),
            "material_norm": r["MaterialNorm"],
            "es_concreto"  : bool(r["EsConcreto"]),
        })
    return rows


def build_capacitaciones(cap: pd.DataFrame) -> list[dict]:
    rows = []
    for _, r in cap.iterrows():
        rows.append({
            "fecha"        : r["Timestamp"].strftime("%Y-%m-%d"),
            "mes"          : r["MesNombre"],
            "nodo"         : r["Nodo"],
            "carrera"      : r["Carrera"],
            "capacitacion" : r["CAPACITACION"],
            "codigo_hash"  : r["CodigoHash"],
        })
    return rows


def build_docentes(docentes: pd.DataFrame) -> list[dict]:
    doc = docentes.copy()
    doc["NodoLimpio"] = doc["Nodo"].astype(str).str.strip().str.replace(" ", "").str.lower()
    doc["NodoNorm"]   = doc["NodoLimpio"].map(NODO_DOC_MAP).fillna(doc["Nodo"])

    TIPO_APOYO_ORDER = ["Convenio", "Asesor tesis", "Apoyo curso"]
    TIPO_LISTADO_ORDER = {"Apoyo curso": 0, "Convenio": 1, "Asesor tesis": 2}

    def classify_apoyo(apoyo):
        a = safe_str(apoyo).lower()
        if "convenio" in a: return "Convenio"
        if "asesor"   in a: return "Asesor tesis"
        return "Apoyo curso"

    # Key: (codigo, nodo) — agrupa múltiples cursos/apoyos del mismo docente
    grouped = {}
    for _, r in doc.iterrows():
        codigo = safe_str(r.get("Codigo")).strip().lstrip("*")
        nodo   = r["NodoNorm"]
        key    = (codigo, nodo)

        if key not in grouped:
            grouped[key] = {
                "nombre"      : safe_str(r.get("Nombre")),
                "apellido"    : safe_str(r.get("Apellido")),
                "carrera"     : safe_str(r.get("Carrera")),
                "nodo"        : nodo,
                "codigo_hash" : hash_codigo(codigo),
                "cursos"      : [], # lista de {curso, tipo_apoyo, detalle}
            }

        curso   = safe_str(r.get("Curso"))
        tipo    = classify_apoyo(r.get("Apoyo"))
        detalle = safe_str(r.get("Apoyo"))

        entrada = {"curso": curso, "tipo": tipo, "detalle": detalle}
        if entrada not in grouped[key]["cursos"]:
            grouped[key]["cursos"].append(entrada)

    # Ordenar cursos por tipo dentro de cada docente y derivar tipo principal
    rows = []
    for entry in grouped.values():
        entry["cursos"].sort(
            key=lambda c: TIPO_APOYO_ORDER.index(c["tipo"])
                          if c["tipo"] in TIPO_APOYO_ORDER else 99
        )
        tipos_unicos = list(dict.fromkeys(c["tipo"] for c in entry["cursos"]))
        entry["tipos_apoyo"] = tipos_unicos       # todos los tipos (para badges)
        entry["tipo_apoyo"] = min(
            (c["tipo"] for c in entry["cursos"]),
            key=lambda t: TIPO_LISTADO_ORDER.get(t, 99)
        )
        rows.append(entry)

    rows.sort(key=lambda d: (
        TIPO_LISTADO_ORDER.get(d["tipo_apoyo"], 9),
        d["nodo"],
        d["apellido"],
        d["nombre"]
    ))
    return rows


def build_convenios(cursos: pd.DataFrame) -> list[dict]:
    conv = cursos[cursos["CONVENIO"].astype(str).str.upper() == "SI"].copy()
    conv["NodoNorm"] = (
        conv["Nodo"].fillna("Sin asignar").astype(str).str.strip().str.upper()
        .map(NODO_CURSO_MAP).fillna("Sin asignar")
    )
    conv["FECHA INICIO DE CONVENIO"] = pd.to_datetime(
        conv["FECHA INICIO DE CONVENIO"], dayfirst=True, errors="coerce"
    ).dt.strftime("%Y-%m-%d").fillna("")

    rows = []
    for _, r in conv.iterrows():
        rows.append({
            "codigo"        : safe_str(r.get("CODIGO")),
            "nombre"        : safe_str(r.get("NOMBRE")),
            "nodo"          : r["NodoNorm"],
            "fecha_convenio": r["FECHA INICIO DE CONVENIO"],
            "notas"         : safe_str(r.get("Notas")),
        })
    return rows


# ─── 5. Main ─────────────────────────────────────────────────────────────────

def compute_output(sheets: dict) -> dict:
    uso      = enrich_uso(sheets["Registro de Uso"], sheets["Usuarios"])
    cap      = enrich_cap(sheets["REGISTRO DE CAPACITACION"], sheets["Usuarios"])
    docentes = sheets["Docentes"]
    usuarios = sheets["Usuarios"]
    cursos   = sheets["CURSOS PUCP"]

    meses_con_actividad = sorted(
        uso["MesNombre"].dropna().unique().tolist(),
        key=lambda m: MESES_ORDER.index(m) if m in MESES_ORDER else 99
    )

    return {
        "atenciones"        : build_atenciones(uso),
        "capacitaciones"    : build_capacitaciones(cap),
        "docentes_vinculados": build_docentes(docentes),
        "convenios"         : build_convenios(cursos),
        "referencia": {
            "nodos"               : NODOS,
            "meses_con_actividad" : meses_con_actividad,
            "meses_order"         : MESES_ORDER,
            "equipos_concreto"    : sorted(EQUIPOS_CONCRETO),
            "tipos_alumno"        : TIPOS_ALUMNO,
            "tipos_doc_jp"        : TIPOS_DOC_JP,
            "resumen_usuarios": {
                "total_usuarios_registrados": int(len(usuarios)),
                "total_alumnos_pregrado"    : int((usuarios["Tipo de Usuario"] == "ESTUDIANTE PREGRADO").sum()),
                "total_alumnos_maestria"    : int((usuarios["Tipo de Usuario"] == "ESTUDIANTE MAESTRIA").sum()),
                "total_docentes_registrados": int((usuarios["Tipo de Usuario"] == "DOCENTE").sum()),
            },
            "ultima_actualizacion": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }


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

    print("\n[3/3] Construyendo dataset...")
    output = compute_output(sheets)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\n{'─'*50}")
    print(f"  Atenciones registradas     : {len(output['atenciones'])}")
    print(f"  Capacitaciones registradas : {len(output['capacitaciones'])}")
    print(f"  Docentes vinculados        : {len(output['docentes_vinculados'])}")
    print(f"  Convenios establecidos     : {len(output['convenios'])}")
    print(f"  Meses con actividad        : {', '.join(output['referencia']['meses_con_actividad'])}")
    print(f"  Guardado en               : {OUTPUT_PATH}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()
