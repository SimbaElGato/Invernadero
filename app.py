import gradio as gr
import requests
import pandas as pd
import numpy as np
import json, os, time, queue, threading, warnings, uuid
from pathlib import Path
from datetime import datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

ultimoEnvioTelegram = 0

def enviarAlertaTelegram(estado, bateriaPct, alertas, temp, suelo):
    global ultimoEnvioTelegram
    ahora = time.time()

    if ahora - ultimoEnvioTelegram < 10:
        return
    ultimoEnvioTelegram = ahora
    token = os.environ.get("TELEGRAM_TOKEN", "")
    chatId = os.environ.get("TELEGRAM_CHAT_ID", "5566819758")
    if not token:
        return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    detalleAlertas = "\n• ".join(alertas) if isinstance(alertas, list) else str(alertas)
    mensaje = (
        f"🚨 ¡Alerta en el Invernadero Off-Grid!\n\n"
        f"⚡ Estado: {estado}\n"
        f"🔋 Batería: {bateriaPct:.1f}%\n"
        f"🌡️ Temp: {temp}°C | 🌱 Suelo: {suelo}%\n"
        f"⚠️ Bloqueos:\n• {detalleAlertas}"
    )

    def _enviar():
        try:
            requests.post(url, json={"chat_id": chatId, "text": mensaje}, timeout=4)
        except Exception:
            pass

    threading.Thread(target=_enviar, daemon=True).start()

warnings.filterwarnings("ignore", category=RuntimeWarning)

# ---------- Fuente 1: invernadero real (ThingSpeak) ----------
CHANNEL_ID = 972587
N_REGISTROS = 2000
MAX_REGISTROS_THINGSPEAK = 8000
MIN_REGISTROS = 10
URL_FEED = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json"

# ---------- Fuente 2: Open-Meteo (MISMA ubicación del canal) ----------
LATITUD = 25.9216775
LONGITUD = 89.4657639
URL_OM_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
URL_OM_FORECAST = "https://api.open-meteo.com/v1/forecast"
OM_HOURLY = "shortwave_radiation,cloud_cover,precipitation,is_day"

TZ_COLOMBIA = ZoneInfo("America/Bogota")

# ---------- Campos del canal 972587 ----------
COLUMNAS = {"field1": "temp", "field2": "hum_amb", "field3": "suelo",
            "field4": "temp_suelo", "field5": "wifi"}
CAMPOS_INFO = {
    "temp": ("Temperatura del aire", "°C", "field1"),
    "hum_amb": ("Humedad del aire", "%", "field2"),
    "suelo": ("Humedad del suelo", "%", "field3"),
    "temp_suelo": ("Temperatura del suelo", "°C", "field4"),
    "wifi": ("Intensidad de señal Wi-Fi", "dBm / escala del canal", "field5"),
}
MINIMAS = ["temp", "hum_amb", "suelo", "temp_suelo"]
LIMITES_FISICOS = {"temp": (-20, 70), "hum_amb": (0, 100), "temp_suelo": (-10, 70)}

# ---------- Parámetros del modelo ----------
PARAMS_DOC = {
    "area_panel_m2": 1.0, "eficiencia_panel": 0.18, "pr_sistema": 0.75,
    "bateria_cap_wh": 600.0, "reserva_pct": 0.20, "consumo_base_w": 1.0,
    "duracion_ciclo_h": 0.25,
    "umbral_suelo": 35.0, "t_alta": 30.0, "t_baja": 18.0, "rad_baja": 100.0,
    "umbral_lluvia_mm": 0.0, "temp_suelo_alta": 35.0, "hum_aire_alta": 85.0,
    "wifi_dbm_min": -80.0, "wifi_pct_min": 30.0,
    "pot_bomba": 60.0, "pot_ventilador": 40.0, "pot_iluminacion": 80.0, "pot_calefactor": 200.0,
}
PARAMS = dict(PARAMS_DOC)

PARAM_UI = {
    "⚡ Panel solar y batería": [
        ("area_panel_m2", "Área del panel (m²)", 0.2, 5, 0.1),
        ("eficiencia_panel", "Eficiencia del panel (0.18 = 18 %)", 0.05, 0.30, 0.01),
        ("pr_sistema", "Factor de rendimiento PR", 0.4, 1.0, 0.01),
        ("bateria_cap_wh", "Capacidad de la batería (Wh)", 100, 2000, 50),
        ("reserva_pct", "Reserva mínima (0.20 = 20 %)", 0.0, 0.5, 0.05),
        ("consumo_base_w", "Consumo base de la electrónica (W)", 0, 10, 0.5),
        ("duracion_ciclo_h", "Tiempo simulado por ciclo (h)", 0.05, 1.0, 0.05),
    ],
    "🎚️ Umbrales de decisión": [
        ("umbral_suelo", "Regar si humedad del suelo < (%)", 5, 80, 1),
        ("t_alta", "Ventilador si temperatura > (°C)", 20, 45, 1),
        ("t_baja", "Calefactor si temperatura < (°C)", 5, 25, 1),
        ("rad_baja", "Iluminar si radiación < (W/m²), solo de día", 10, 400, 10),
        ("umbral_lluvia_mm", "Lluvia que bloquea el riego (mm/h)", 0, 5, 0.1),
        ("temp_suelo_alta", "Evento si temp. del suelo > (°C)", 20, 50, 1),
        ("hum_aire_alta", "Evento si humedad del aire > (%)", 50, 100, 1),
    ],
    "🔌 Potencia de los actuadores (W)": [
        ("pot_bomba", "Bomba de agua", 10, 300, 10),
        ("pot_ventilador", "Ventilador", 10, 300, 10),
        ("pot_iluminacion", "Iluminación artificial", 10, 500, 10),
        ("pot_calefactor", "Calefactor", 50, 1000, 10),
    ],
}
PARAM_KEYS = [k for grupo in PARAM_UI.values() for (k, *_) in grupo]

ARCHIVO_CSV = (Path.cwd() / "datos_sincronizados.csv").resolve()
ARCHIVO_SESION = (Path.cwd() / "telemetria_procesada_invernadero.json").resolve()

DATOS = {"df": None, "bruto": None, "canal": {}, "notas": [], "info": {}, "registros": []}


def lista_actuadores():
    h = PARAMS["duracion_ciclo_h"]
    return [
        ("bomba", "Bomba de agua", "🚿", PARAMS["pot_bomba"], h),
        ("ventilador", "Ventilador", "🌀", PARAMS["pot_ventilador"], h),
        ("iluminacion", "Iluminación artificial", "💡", PARAMS["pot_iluminacion"], h),
        ("calefactor", "Calefactor", "🔥", PARAMS["pot_calefactor"], h),
    ]


# ============================================================
#  DATOS: captura (ThingSpeak) → limpieza → sincronización (Open-Meteo) → CSV
# ============================================================

def es_nan(v):
    return v is None or (isinstance(v, float) and np.isnan(v))


def descargar_thingspeak(n):
    n = int(max(MIN_REGISTROS, min(int(n), MAX_REGISTROS_THINGSPEAK)))
    r = requests.get(URL_FEED, params={"results": n}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    feeds = payload.get("feeds", []) if isinstance(payload, dict) else []
    if not feeds:
        raise ValueError("ThingSpeak respondió, pero no devolvió registros.")
    bruto = pd.DataFrame(feeds)
    bruto["created_at"] = pd.to_datetime(bruto["created_at"], errors="coerce", utc=True)
    for campo in COLUMNAS:
        if campo not in bruto.columns:
            bruto[campo] = np.nan
        bruto[campo] = pd.to_numeric(bruto[campo], errors="coerce")
    bruto = bruto.dropna(subset=["created_at"]).sort_values("created_at").reset_index(drop=True)
    return bruto, payload.get("channel", {})


def preparar_datos(bruto):
    notas = []
    d = bruto.copy()
    for campo, col in COLUMNAS.items():
        d[col] = d[campo]
    for col, (lo, hi) in LIMITES_FISICOS.items():
        fuera = d[col].notna() & ((d[col] < lo) | (d[col] > hi))
        if fuera.any():
            notas.append(f"{int(fuera.sum())} valores de «{CAMPOS_INFO[col][0]}» fuera de rango físico: descartados.")
        d.loc[fuera, col] = np.nan
    s = d["suelo"].dropna()
    if not s.empty and (s.min() < 0 or s.max() > 100) and s.max() > s.min():
        lo, hi = float(s.min()), float(s.max())
        d["suelo"] = (d["suelo"] - lo) / (hi - lo) * 100.0
        notas.append(f"Humedad del suelo llegó entre {lo:.1f} y {hi:.1f}: se escaló a 0-100 %. Revisar calibración.")
    antes = len(d)
    d = d.dropna(subset=MINIMAS).reset_index(drop=True)
    if antes - len(d):
        notas.append(f"{antes - len(d)} registros descartados por no tener las variables mínimas (no se inventan valores).")
    return d, notas


def url_open_meteo(hora_utc, api="archive"):
    fecha = hora_utc.strftime("%Y-%m-%d")
    params = {"latitude": LATITUD, "longitude": LONGITUD, "start_date": fecha, "end_date": fecha,
              "hourly": OM_HOURLY, "timezone": "GMT"}
    return f"{URL_OM_ARCHIVE if api == 'archive' else URL_OM_FORECAST}?{urlencode(params)}"


def pedir_open_meteo(base, ini, fin):
    params = {"latitude": LATITUD, "longitude": LONGITUD, "start_date": ini, "end_date": fin,
              "hourly": OM_HOURLY, "timezone": "GMT"}
    for intento in range(3):
        try:
            r = requests.get(base, params=params, timeout=40)
            if r.status_code == 200:
                h = r.json().get("hourly")
                if h:
                    om = pd.DataFrame(h)
                    om["time"] = pd.to_datetime(om["time"], utc=True)
                    return om
                return None
            if r.status_code in (400, 404):
                return None
        except Exception:
            pass
        time.sleep(1.5 * (intento + 1))
    return None


def consultar_open_meteo(ini, fin):
    partes, n_pet = [], 1
    om = pedir_open_meteo(URL_OM_ARCHIVE, ini, fin)
    if om is not None:
        ok = om[om["shortwave_radiation"].notna()].copy()
        ok["fuente_api"] = "archive"
        partes.append(ok)
        dias_ok = set(ok["time"].dt.strftime("%Y-%m-%d"))
    else:
        dias_ok = set()
    todos = pd.date_range(ini, fin, freq="D").strftime("%Y-%m-%d")
    faltan = [d for d in todos if d not in dias_ok]
    if faltan:
        n_pet += 1
        fc = pedir_open_meteo(URL_OM_FORECAST, faltan[0], faltan[-1])
        if fc is not None:
            fc = fc[fc["shortwave_radiation"].notna()].copy()
            fc["fuente_api"] = "forecast"
            partes.append(fc)
    return (pd.concat(partes, ignore_index=True) if partes else None), n_pet


def sincronizar_open_meteo(d):
    d = d.copy()
    d["hora_ref_utc"] = d["created_at"].dt.round("h").dt.as_unit("ns")
    d["url_open_meteo"] = [url_open_meteo(h) for h in d["hora_ref_utc"]]
    fechas = sorted(d["hora_ref_utc"].dt.strftime("%Y-%m-%d").unique())
    bloques, actual = [], []
    for f in fechas:
        if actual and (pd.Timestamp(f) - pd.Timestamp(actual[0])).days > 30:
            bloques.append(actual)
            actual = []
        actual.append(f)
    bloques.append(actual)

    partes, peticiones = [], 0
    for b in bloques:
        om, n_pet = consultar_open_meteo(b[0], b[-1])
        peticiones += n_pet
        if om is not None:
            partes.append(om)
    if not partes:
        raise ValueError("Open-Meteo no devolvió datos para las fechas de los registros. "
                         "Revisa la conexión o que las fechas no sean demasiado antiguas/futuras.")
    om_all = pd.concat(partes, ignore_index=True).drop_duplicates(subset=["time"])
    om_all = om_all.rename(columns={"time": "hora_ref_utc", "shortwave_radiation": "rad_wm2",
                                    "cloud_cover": "nubosidad", "precipitation": "precip_om",
                                    "is_day": "es_dia"})
    om_all["hora_ref_utc"] = om_all["hora_ref_utc"].dt.as_unit("ns")
    df = d.merge(om_all, on="hora_ref_utc", how="left")
    sin_rad = int(df["rad_wm2"].isna().sum())
    df = df.dropna(subset=["rad_wm2"]).sort_values("created_at").reset_index(drop=True)
    df["es_dia"] = df["es_dia"].fillna((df["rad_wm2"] > 0).astype(float))
    df["precip_om"] = df["precip_om"].fillna(0.0)
    notas = []
    if sin_rad:
        notas.append(f"{sin_rad} registros descartados por no tener radiación de Open-Meteo en su hora.")
    info = {"dias": len(fechas), "peticiones_om": peticiones,
            "apis": sorted(om_all["fuente_api"].unique().tolist())}
    return df, info, notas


def normalizar_registro(r):
    return {"created_at": r["created_at"], "temp": r["temp"], "hum_amb": r["hum_amb"],
            "suelo": r["suelo"], "temp_suelo": r["temp_suelo"], "wifi": r.get("wifi", np.nan),
            "rad": r["rad_wm2"], "nubosidad": r.get("nubosidad", np.nan),
            "precip": r.get("precip_om", 0.0), "es_dia": r["es_dia"]}


def cargar_datos(n):
    bruto, canal = descargar_thingspeak(n)
    d, notas = preparar_datos(bruto)
    df, info, notas_om = sincronizar_open_meteo(d)
    if len(df) < MIN_REGISTROS:
        raise ValueError(f"Solo quedaron {len(df)} registros válidos (mínimo {MIN_REGISTROS}).")
    df.to_csv(ARCHIVO_CSV, index=False, encoding="utf-8")
    info.update(n_pedidos=int(n), n_recibidos=len(bruto), n_usados=len(df))
    DATOS.update(bruto=bruto, df=df, canal=canal, notas=notas + notas_om, info=info,
                 registros=[normalizar_registro(r) for r in df.to_dict("records")])
    return len(df)


def tabla_html(encabezados, filas):
    th = "".join(f'<th style="background:#1E3A5F;color:#FFFFFF;padding:8px 10px;text-align:left;font-size:12px;">{h}</th>'
                 for h in encabezados)
    trs = ""
    for i, fila in enumerate(filas):
        bg = "#FFFFFF" if i % 2 == 0 else "#EEF2F7"
        tds = "".join(f'<td style="background:{bg};color:#0F172A;padding:7px 10px;font-size:13px;border-top:1px solid #CBD5E1;">{c}</td>'
                      for c in fila)
        trs += f"<tr>{tds}</tr>"
    return (f'<div style="overflow-x:auto;border:1px solid #CBD5E1;border-radius:10px;">'
            f'<table style="width:100%;border-collapse:collapse;"><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>')


def tarjeta(titulo, valor, sub=""):
    return (f'<div style="flex:1 1 170px;background:#FFFFFF;border:1px solid #CBD5E1;border-radius:12px;padding:12px 14px;">'
            f'<div style="font-size:11px;font-weight:700;letter-spacing:.8px;color:#475569;text-transform:uppercase;">{titulo}</div>'
            f'<div style="font-size:22px;font-weight:800;color:#0F172A;margin-top:2px;">{valor}</div>'
            f'<div style="font-size:12px;color:#475569;margin-top:2px;">{sub}</div></div>')


def fnum(v, d=2):
    return "—" if es_nan(v) else f"{float(v):.{d}f}"


def construir_resumen_datos():
    df, bruto, info = DATOS["df"], DATOS["bruto"], DATOS["info"]
    ini, fin = df["created_at"].min(), df["created_at"].max()
    difs = df["created_at"].diff().dt.total_seconds().dropna()
    med = float(difs.median()) if len(difs) else float("nan")
    tarjetas = (
        tarjeta("Registros usados", f"{len(df):,}", f"de {info['n_recibidos']:,} recibidos")
        + tarjeta("Periodo", f"{ini:%Y-%m-%d}", f"hasta {fin:%Y-%m-%d} (UTC)")
        + tarjeta("Intervalo original", f"{med:.0f} s", "mediana entre registros")
        + tarjeta("Open-Meteo", f"{info['dias']} día(s)", f"{info['peticiones_om']} petición(es) · {', '.join(info['apis'])}")
    )
    filas = []
    for col, (nombre, unidad, campo) in CAMPOS_INFO.items():
        s = df[col]
        filas.append([f"<b>{nombre}</b>", campo, unidad, fnum(s.min()), fnum(s.mean()), fnum(s.max()),
                      int(bruto[campo].isna().sum())])
    for col, nombre, unidad in (("rad_wm2", "Radiación solar", "W/m²"), ("nubosidad", "Nubosidad", "%"),
                                ("precip_om", "Precipitación", "mm/h")):
        s = df[col]
        filas.append([f"<b>{nombre}</b>", "Open-Meteo", unidad, fnum(s.min()), fnum(s.mean()), fnum(s.max()), 0])
    tabla = tabla_html(["Variable", "Origen", "Unidad", "Mín.", "Prom.", "Máx.", "Faltantes"], filas)
    ubic = (f'<div style="font-size:13px;color:#0F172A;margin:10px 0 12px;">📍 Canal <b>{CHANNEL_ID}</b> · '
            f'Lat <b>{LATITUD}</b>, Lon <b>{LONGITUD}</b> · Open-Meteo usa las <b>mismas coordenadas</b> y la <b>misma hora</b> de cada registro.</div>')
    return f'<div style="display:flex;flex-wrap:wrap;gap:10px;">{tarjetas}</div>{ubic}{tabla}'


def construir_notas_html():
    notas = DATOS["notas"] or ["Sin observaciones: todos los registros cumplieron las validaciones."]
    li = "".join(f'<li style="margin:4px 0;color:#0F172A;">{n}</li>' for n in notas)
    return f'<ul style="padding-left:20px;font-size:13px;">{li}</ul>'


def construir_detalle_html():
    df = DATOS["df"]
    campos = tabla_html(
        ["Campo", "Variable", "Uso en el sistema"],
        [["field1", "Temperatura del aire", "Ventilador y calefactor"],
         ["field2", "Humedad del aire", "Contexto y evento por humedad alta"],
         ["field3", "Humedad del suelo", "Riego"],
         ["field4", "Temperatura del suelo", "Evento por suelo demasiado caliente"],
         ["field5", "Intensidad de señal Wi-Fi", "Evento por conectividad débil"],
         ["Open-Meteo", "Radiación, nubosidad, precipitación", "Generación del panel, iluminación y bloqueo por lluvia"]])
    cols = ["created_at", "hora_ref_utc", "temp", "hum_amb", "suelo", "temp_suelo", "wifi", "rad_wm2", "nubosidad", "precip_om", "es_dia"]
    m = df[cols].head(8).copy()
    for c in ("created_at", "hora_ref_utc"):
        m[c] = m[c].dt.strftime("%Y-%m-%d %H:%M")
    muestra = tabla_html(cols, [[("" if es_nan(x) else (round(x, 2) if isinstance(x, float) else x)) for x in fila]
                                for fila in m.values.tolist()])
    ej = df["url_open_meteo"].iloc[0]
    return (f'<div style="font-weight:800;color:#0F172A;margin:4px 0 6px;">Campos y uso</div>{campos}'
            f'<div style="font-weight:800;color:#0F172A;margin:14px 0 6px;">Primeros 8 registros sincronizados</div>{muestra}'
            f'<div style="font-weight:800;color:#0F172A;margin:14px 0 6px;">Ruta Open-Meteo del primer registro (cada registro tiene la suya en el CSV)</div>'
            f'<div style="background:#0F172A;color:#E2E8F0;border-radius:8px;padding:10px;font-family:monospace;font-size:12px;word-break:break-all;">{ej}</div>')


# ============================================================
#  MOTOR DE DECISIÓN (borde simulado): energía disponible vs requerida
# ============================================================

def media(vals):
    a = np.array([v for v in vals if not es_nan(v)], dtype=float)
    return float(a.mean()) if a.size else float("nan")


def r3(v):
    return None if es_nan(v) else round(float(v), 3)


def calcular_generacion(rad_wm2):
    p = float(rad_wm2) * PARAMS["area_panel_m2"] * PARAMS["eficiencia_panel"] * PARAMS["pr_sistema"]
    return p, p * PARAMS["duracion_ciclo_h"]


def wifi_debil(w):
    if es_nan(w):
        return False
    return w < PARAMS["wifi_dbm_min"] if w <= 0 else w < PARAMS["wifi_pct_min"]


def promediar_ventana(buffer):
    campos = ["temp", "hum_amb", "suelo", "temp_suelo", "wifi", "rad", "nubosidad", "precip", "es_dia"]
    prom = {c: media([r.get(c, np.nan) for r in buffer]) for c in campos}
    prom.update(t_inicio=buffer[0].get("created_at"), t_fin=buffer[-1].get("created_at"), n=len(buffer))
    return prom


def evaluar_ciclo(prom, bateria_wh):
    P = PARAMS
    cap = P["bateria_cap_wh"]
    reserva = P["reserva_pct"] * cap
    rad = float(prom["rad"])
    p_gen, e_gen = calcular_generacion(rad)
    e_base = P["consumo_base_w"] * P["duracion_ciclo_h"]
    e_disp_ini = float(bateria_wh) + e_gen - reserva
    e_disp = e_disp_ini

    temp, suelo, hum = prom["temp"], prom["suelo"], prom["hum_amb"]
    tsuelo, wifi = prom["temp_suelo"], prom["wifi"]
    lluvia = (not es_nan(prom["precip"])) and prom["precip"] > P["umbral_lluvia_mm"]
    es_dia = (not es_nan(prom["es_dia"])) and prom["es_dia"] >= 0.5

    necesidad = {
        "bomba": suelo < P["umbral_suelo"] and not lluvia,
        "ventilador": temp > P["t_alta"],
        "iluminacion": rad < P["rad_baja"] and es_dia,
        "calefactor": temp < P["t_baja"],
    }
    motivo = {
        "bomba": f"humedad del suelo {suelo:.1f} % < {P['umbral_suelo']:.0f} %",
        "ventilador": f"temperatura {temp:.1f} °C > {P['t_alta']:.0f} °C",
        "iluminacion": f"radiación {rad:.0f} W/m² < {P['rad_baja']:.0f} W/m² de día",
        "calefactor": f"temperatura {temp:.1f} °C < {P['t_baja']:.0f} °C",
    }

    eventos, alertas = [], []
    if suelo < P["umbral_suelo"]:
        eventos.append(f"Suelo seco: humedad {suelo:.1f} % (umbral {P['umbral_suelo']:.0f} %).")
    if lluvia:
        eventos.append(f"Lluvia detectada ({prom['precip']:.2f} mm/h)" +
                       (": riego omitido." if suelo < P["umbral_suelo"] else "."))
    if temp > P["t_alta"]:
        eventos.append(f"Temperatura alta: {temp:.1f} °C.")
    if temp < P["t_baja"]:
        eventos.append(f"Temperatura baja: {temp:.1f} °C.")
    if rad < P["rad_baja"] and es_dia:
        eventos.append(f"Radiación baja en horario diurno: {rad:.0f} W/m².")
    if not es_nan(hum) and hum > P["hum_aire_alta"]:
        eventos.append(f"Humedad del aire alta: {hum:.0f} %.")
    if not es_nan(tsuelo) and tsuelo > P["temp_suelo_alta"]:
        eventos.append(f"Suelo demasiado caliente: {tsuelo:.1f} °C.")
    if wifi_debil(wifi):
        eventos.append(f"Señal Wi-Fi débil ({wifi:.0f}): riesgo de perder comunicación con la nube.")

    act = {k: False for k, *_ in lista_actuadores()}
    consumo, activados, bloqueados = 0.0, [], []
    for clave, nombre, icono, pot, horas in lista_actuadores():
        if not necesidad[clave]:
            continue
        e_req = pot * horas
        if e_disp >= e_req:
            e_disp -= e_req
            consumo += e_req
            act[clave] = True
            activados.append((clave, nombre, icono, e_req))
        else:
            bloqueados.append(clave)
            rad_txt = (f"Radiación solar actual baja ({rad:.0f} W/m²)." if rad < P["rad_baja"]
                       else f"Radiación solar actual: {rad:.0f} W/m².")
            if clave == "bomba":
                alertas.append(f"ALERTA: Energía insuficiente para iniciar el riego. {rad_txt}")
            else:
                alertas.append(f"ALERTA: Energía insuficiente para activar {nombre.lower()} "
                               f"({e_req:.0f} Wh requeridos, {max(e_disp, 0):.1f} Wh disponibles). {rad_txt}")
            eventos.append(f"Acción bloqueada por energía: {nombre}.")

    bat_fin = min(cap, max(0.0, float(bateria_wh) + e_gen - e_base - consumo))
    pct = bat_fin / cap * 100.0
    acciones = []
    for clave, nombre, icono, e_req in activados:
        if clave == "bomba":
            acciones.append(f"{icono} Riego activado exitosamente ({motivo[clave]}). Nivel de batería restante: {pct:.1f} %.")
        else:
            acciones.append(f"{icono} {nombre} ENCENDIDO ({motivo[clave]}). Consume {e_req:.0f} Wh.")
    if not acciones and not alertas:
        acciones.append("✅ No se requiere ninguna acción. El invernadero está en condiciones normales.")

    demanda = sum(p * h for k, _, _, p, h in lista_actuadores() if necesidad[k])

    if alertas:
        enviarAlertaTelegram("ALERTA", pct, alertas, round(temp, 1), round(suelo, 1))

    return {
        "estado": "ALERTA" if alertas else ("ACTUANDO" if any(act.values()) else "ESTABLE"),
        "actuadores": act, "necesidad": {k: bool(v) for k, v in necesidad.items()},
        "bloqueados": bloqueados, "eventos": eventos, "alertas": alertas, "acciones": acciones,
        "entradas": {"suelo": suelo, "temp": temp, "rad": rad, "lluvia": bool(lluvia), "es_dia": bool(es_dia)},
        "energia": {
            "radiacion_wm2": rad, "potencia_generada_w": p_gen, "duracion_ciclo_h": P["duracion_ciclo_h"],
            "energia_generada_wh": e_gen, "bateria_inicial_wh": float(bateria_wh), "reserva_wh": reserva,
            "energia_disponible_wh": max(e_disp_ini, 0.0), "energia_requerida_wh": demanda,
            "energia_consumida_wh": consumo, "consumo_base_wh": e_base,
            "bateria_final_wh": bat_fin, "bateria_pct": pct,
        },
    }


def estado_inicial(bateria_wh):
    cap = PARAMS["bateria_cap_wh"]
    return {
        "estado": "ESTABLE", "actuadores": {k: False for k, *_ in lista_actuadores()},
        "necesidad": {k: False for k, *_ in lista_actuadores()}, "bloqueados": [], "eventos": [], "alertas": [],
        "acciones": ["⏳ Esperando el primer ciclo de control del borde."], "entradas": {},
        "energia": {"radiacion_wm2": 0.0, "potencia_generada_w": 0.0, "duracion_ciclo_h": PARAMS["duracion_ciclo_h"],
                    "energia_generada_wh": 0.0, "bateria_inicial_wh": float(bateria_wh),
                    "reserva_wh": PARAMS["reserva_pct"] * cap, "energia_disponible_wh": 0.0,
                    "energia_requerida_wh": 0.0, "energia_consumida_wh": 0.0, "consumo_base_wh": 0.0,
                    "bateria_final_wh": float(bateria_wh), "bateria_pct": float(bateria_wh) / cap * 100.0},
    }


# ============================================================
#  PERSISTENCIA JSON: un registro por ciclo de control
# ============================================================
SESION = {
    "metadata": {
        "channel_id_origen": CHANNEL_ID, "ubicacion": {"latitud": LATITUD, "longitud": LONGITUD},
        "fuentes": ["ThingSpeak", "Open-Meteo"],
        "tipo": "gemelo digital: replay de telemetría real sincronizada con radiación solar",
        "persistencia": "Un registro por ciclo de control. No se guardan las lecturas individuales.",
        "zona_horaria_registros": "America/Bogota",
    },
    "ciclos_control": [],
}


def guardar_sesion_json():
    with ARCHIVO_SESION.open("w", encoding="utf-8") as f:
        json.dump(SESION, f, ensure_ascii=False, indent=2, default=str)
    return str(ARCHIVO_SESION)


def construir_registro(prom, dec, intervalo_control, origen="replay"):
    en = dec["energia"]
    ti, tf = prom.get("t_inicio"), prom.get("t_fin")
    return {
        "ciclo": len(SESION["ciclos_control"]) + 1,
        "origen": origen,
        "fecha_hora_actual": datetime.now(TZ_COLOMBIA).isoformat(timespec="seconds"),
        "ventana_historica_utc": {"inicio": None if ti is None else str(ti), "fin": None if tf is None else str(tf)},
        "ventana_control_segundos": int(intervalo_control),
        "cantidad_lecturas_promediadas": int(prom["n"]),
        "sensores_promedio": {
            "temperatura_aire_c": r3(prom["temp"]), "humedad_aire_pct": r3(prom["hum_amb"]),
            "humedad_suelo_pct": r3(prom["suelo"]), "temperatura_suelo_c": r3(prom["temp_suelo"]),
            "senal_wifi": r3(prom["wifi"]),
        },
        "open_meteo": {
            "radiacion_solar_wm2": r3(prom["rad"]), "nubosidad_pct": r3(prom["nubosidad"]),
            "precipitacion_mm_h": r3(prom["precip"]),
            "es_dia": bool(not es_nan(prom["es_dia"]) and prom["es_dia"] >= 0.5),
        },
        "energia": {
            "potencia_generada_w": r3(en["potencia_generada_w"]), "duracion_ciclo_h": en["duracion_ciclo_h"],
            "energia_generada_wh": r3(en["energia_generada_wh"]), "bateria_inicial_wh": r3(en["bateria_inicial_wh"]),
            "energia_disponible_wh": r3(en["energia_disponible_wh"]), "energia_requerida_wh": r3(en["energia_requerida_wh"]),
            "energia_consumida_wh": r3(en["energia_consumida_wh"]), "bateria_final_wh": r3(en["bateria_final_wh"]),
            "bateria_pct": r3(en["bateria_pct"]),
        },
        "actuadores": {k: bool(v) for k, v in dec["actuadores"].items()},
        "actuadores_bloqueados_por_energia": list(dec["bloqueados"]),
        "estado": dec["estado"], "eventos": list(dec["eventos"]), "alertas": list(dec["alertas"]),
        "acciones": list(dec["acciones"]),
        "parametros": {k: PARAMS[k] for k in PARAM_KEYS},
    }


def registrar_ciclo_control(registro):
    SESION["ciclos_control"].append(registro)
    guardar_sesion_json()


def descargar_json_sesion():
    return guardar_sesion_json()


def descargar_csv_sincronizado():
    if DATOS["df"] is not None:
        DATOS["df"].to_csv(ARCHIVO_CSV, index=False, encoding="utf-8")
    return str(ARCHIVO_CSV)


# ============================================================
#  CONCURRENCIA: control local + envío simulado a la nube (hilo + cola)
# ============================================================
COLA_NUBE = queue.Queue()
LOCK_NUBE = threading.Lock()
ESTADO_NUBE = {"estado": "INICIANDO", "detalle": "Preparando el hilo de comunicación.", "hilo_id": None,
               "paquete_actual": None, "enviados": 0, "ultimo_envio": "Todavía no hay envíos finalizados.",
               "sesion_id": 0}
ESTADO_REPLAY = {"estado": "PREPARADO", "detalle": "El replay todavía no ha iniciado.", "hilo_id": None}


def actualizar_estado_nube(estado, detalle, paquete=None):
    with LOCK_NUBE:
        ESTADO_NUBE.update(estado=estado, detalle=detalle, paquete_actual=paquete)


def actualizar_estado_nube_si_actual(sid, estado, detalle, paquete=None):
    with LOCK_NUBE:
        if sid != ESTADO_NUBE["sesion_id"]:
            return False
        ESTADO_NUBE.update(estado=estado, detalle=detalle, paquete_actual=paquete)
        return True


def reiniciar_comunicacion_nube():
    with LOCK_NUBE:
        ESTADO_NUBE["sesion_id"] += 1
        ESTADO_NUBE.update(estado="ESPERANDO DATOS", detalle="Sesión reiniciada; espera el primer ciclo.",
                           paquete_actual=None, enviados=0, ultimo_envio="Todavía no hay envíos finalizados.")
    while True:
        try:
            COLA_NUBE.get_nowait()
            COLA_NUBE.task_done()
        except queue.Empty:
            break


def trabajador_nube():
    with LOCK_NUBE:
        ESTADO_NUBE["hilo_id"] = threading.get_ident()
    while True:
        actualizar_estado_nube("ESPERANDO DATOS", "El hilo espera en la cola a que termine un ciclo de control.")
        p = COLA_NUBE.get()
        n, demora, sid = p["numero"], float(p["demora"]), p["sesion_id"]
        actualizar_estado_nube_si_actual(sid, "EJECUTÁNDOSE", f"Preparando el paquete del ciclo {n}.", n)
        time.sleep(0.5)
        actualizar_estado_nube_si_actual(sid, "ESPERANDO RED",
                                         f"El paquete {n} fue enviado; se simula una respuesta de {demora:.0f} s.", n)
        time.sleep(demora)
        with LOCK_NUBE:
            if sid == ESTADO_NUBE["sesion_id"]:
                ESTADO_NUBE.update(estado="FINALIZADO", detalle=f"La nube confirmó el paquete {n}.", paquete_actual=n,
                                   ultimo_envio=datetime.now(TZ_COLOMBIA).strftime("%Y-%m-%d %H:%M:%S"))
                ESTADO_NUBE["enviados"] += 1
        COLA_NUBE.task_done()
        time.sleep(1.0)


HILO_NUBE = threading.Thread(target=trabajador_nube, name="comunicacion_nube", daemon=True)
HILO_NUBE.start()


def encolar_envio_nube(registro, demora_red):
    with LOCK_NUBE:
        sid = ESTADO_NUBE["sesion_id"]
    COLA_NUBE.put({"numero": registro["ciclo"], "demora": float(demora_red), "sesion_id": sid})


# ============================================================
#  SIMULACIÓN: replay + control periódico + simulador manual
# ============================================================
LOCK_REPLAY = threading.RLock()

REPLAY = {
    "playing": False, "index": 0, "seg_control": 0, "seg_emision": 0, "buffer": [],
    "control_state": None, "decision": None, "ultima_evaluacion": "Todavía no hay evaluación.",
    "fase": "telemetria", "fase_ticks": 0, "bateria_wh": 0.0, "bateria_inicial_pct": 50.0,
    "vista": {}, "log": [], "hist": [], "mensaje": "Listo. Pulsa Iniciar.",
}

PRESETS = {
    "normal": (25, 65, 55, 24, -60, 650, 0.0, True, 70),
    "evento": (26, 60, 30, 25, -62, 500, 0.0, True, 60),
    "critico": (22, 70, 18, 22, -65, 0, 0.0, False, 20),
}
PRESET_TITULOS = {"normal": "Estado normal", "evento": "Evento: suelo seco con energía suficiente",
                  "critico": "Estado crítico: suelo seco sin energía (noche)"}


def registrar_en_log(ciclo, dec, origen):
    ahora = datetime.now(TZ_COLOMBIA).strftime("%H:%M:%S")
    for a in dec["alertas"]:
        REPLAY["log"].append(("alerta", ahora, ciclo, origen, a))
    for e in dec["eventos"]:
        REPLAY["log"].append(("evento", ahora, ciclo, origen, e))
    if not dec["alertas"] and not dec["eventos"]:
        REPLAY["log"].append(("ok", ahora, ciclo, origen, "Ciclo sin eventos: sistema estable."))
    REPLAY["log"] = REPLAY["log"][-80:]


def reiniciar_estado(bateria_pct=None):
    with LOCK_REPLAY:
        if bateria_pct is not None:
            REPLAY["bateria_inicial_pct"] = float(bateria_pct)
        regs = DATOS["registros"]
        REPLAY.update(playing=False, index=0, seg_control=0, seg_emision=0, fase="telemetria",
                      fase_ticks=0, log=[], hist=[], decision=None, mensaje="Listo. Pulsa Iniciar.")
        REPLAY["bateria_wh"] = REPLAY["bateria_inicial_pct"] / 100.0 * PARAMS["bateria_cap_wh"]
        REPLAY["buffer"] = [regs[0]]
        REPLAY["vista"] = dict(regs[0])
        REPLAY["control_state"] = estado_inicial(REPLAY["bateria_wh"])
        REPLAY["ultima_evaluacion"] = "Todavía no hay evaluación: espera el primer ciclo de control."
        SESION["ciclos_control"].clear()
        reiniciar_comunicacion_nube()
        guardar_sesion_json()


def cerrar_ciclo(prom, intervalo_control, demora_red, bateria_wh, origen):
    dec = evaluar_ciclo(prom, bateria_wh)
    registro = construir_registro(prom, dec, intervalo_control, origen)
    registrar_ciclo_control(registro)
    if demora_red is not None:
        encolar_envio_nube(registro, demora_red)
    registrar_en_log(registro["ciclo"], dec, origen)
    REPLAY["hist"].append({"ciclo": registro["ciclo"], "bat": dec["energia"]["bateria_pct"],
                           "suelo": prom["suelo"], "rad": prom["rad"], "estado": dec["estado"]})
    REPLAY["hist"] = REPLAY["hist"][-120:]
    REPLAY["control_state"] = REPLAY["decision"] = dec
    REPLAY["fase"], REPLAY["fase_ticks"] = "control", 2
    return dec


def ejecutar_ciclo_replay(intervalo_control, demora_red):
    buf = REPLAY["buffer"]
    if not buf:
        return "Sin lecturas nuevas para evaluar."
    prom = promediar_ventana(buf)
    dec = cerrar_ciclo(prom, intervalo_control, demora_red, REPLAY["bateria_wh"], "replay")
    REPLAY["bateria_wh"] = dec["energia"]["bateria_final_wh"]
    REPLAY["buffer"], REPLAY["seg_control"] = [], 0
    msg = (f"Ventana de {prom['n']} lecturas → T={prom['temp']:.1f} °C, suelo={prom['suelo']:.1f} %, "
           f"radiación={prom['rad']:.0f} W/m², batería={dec['energia']['bateria_pct']:.1f} %.")
    REPLAY["ultima_evaluacion"] = msg
    return msg


def avanzar_registro():
    regs = DATOS["registros"]
    if REPLAY["index"] < len(regs) - 1:
        REPLAY["index"] += 1
        r = regs[REPLAY["index"]]
        REPLAY["buffer"].append(r)
        REPLAY["vista"] = dict(r)
        return True
    return False


def ejecutar_manual(temp, hum, suelo, tsuelo, wifi, rad, precip, dia, bat, velocidad, intervalo, demora, titulo="Valores manuales"):
    with LOCK_REPLAY:
        prom = dict(temp=float(temp), hum_amb=float(hum), suelo=float(suelo), temp_suelo=float(tsuelo),
                    wifi=float(wifi), rad=float(rad), nubosidad=float("nan"), precip=float(precip),
                    es_dia=1.0 if dia else 0.0, t_inicio=None, t_fin=None, n=1)
        REPLAY["playing"] = False
        ESTADO_REPLAY.update(estado="PAUSADO", detalle=f"Ciclo manual: {titulo}.", hilo_id=threading.get_ident())
        REPLAY["vista"] = dict(prom, created_at=None)
        dec = cerrar_ciclo(prom, intervalo, None, float(bat) / 100.0 * PARAMS["bateria_cap_wh"], "manual")
        REPLAY["ultima_evaluacion"] = f"{titulo} (batería inicial {float(bat):.0f} %)."
        REPLAY["mensaje"] = f"Ciclo manual ejecutado: {titulo}."
        return construir_salida(velocidad, intervalo)


# ============================================================
#  DASHBOARD
# ============================================================
INK, MUT, LINE = "#0F172A", "#475569", "#CBD5E1"


def fmt(v, dec=1, unidad=""):
    return "—" if es_nan(v) else f"{float(v):.{dec}f}{unidad}"


def kpi(titulo, valor, sub="", color=INK):
    return (f'<div style="flex:1 1 150px;background:#FFFFFF;border:1px solid {LINE};border-radius:12px;padding:12px 14px;">'
            f'<div style="font-size:11px;font-weight:700;letter-spacing:.8px;color:{MUT};text-transform:uppercase;">{titulo}</div>'
            f'<div style="font-size:24px;font-weight:800;color:{color};margin-top:2px;">{valor}</div>'
            f'<div style="font-size:12px;color:{MUT};">{sub}</div></div>')


def gauge(icono, nombre, valor, vmin, vmax, unidad, dec=1, ok=None, marcas=()):
    if es_nan(valor):
        return (f'<div style="margin:9px 0;color:{MUT};font-size:13px;">{icono} {nombre}: sin dato</div>')
    pct = max(0.0, min(1.0, (float(valor) - vmin) / (vmax - vmin))) * 100
    bueno = ok is None or (ok[0] <= float(valor) <= ok[1])
    col = "#15803D" if bueno else "#B45309"
    ticks = "".join(
        f'<div style="position:absolute;left:{max(0, min(100, (m - vmin) / (vmax - vmin) * 100)):.1f}%;top:-2px;width:2px;height:14px;background:{INK};"></div>'
        for m in marcas)
    return (f'<div style="margin:10px 0;"><div style="display:flex;justify-content:space-between;font-size:13px;color:{INK};">'
            f'<span>{icono} {nombre}</span><b style="color:{col};">{float(valor):.{dec}f} {unidad}</b></div>'
            f'<div style="position:relative;background:#E2E8F0;border-radius:999px;height:10px;margin-top:5px;">'
            f'<div style="width:{pct:.1f}%;background:{col};height:10px;border-radius:999px;"></div>{ticks}</div></div>')


def tarjeta_actuador(icono, nombre, activo, bloqueado, necesario, detalle):
    if bloqueado:
        bg, bd, est, col = "#FEE2E2", "#DC2626", "BLOQUEADO · sin energía", "#991B1B"
    elif activo:
        bg, bd, est, col = "#DCFCE7", "#16A34A", "ENCENDIDO", "#166534"
    else:
        bg, bd, est, col = "#F1F5F9", LINE, "APAGADO · no necesario", "#475569"
    return (f'<div style="background:{bg};border:2px solid {bd};border-radius:14px;padding:12px 8px;text-align:center;">'
            f'<div style="font-size:28px;line-height:1;">{icono}</div>'
            f'<div style="font-size:14px;font-weight:800;color:{INK};margin-top:6px;">{nombre}</div>'
            f'<div style="font-size:11px;font-weight:800;color:{col};margin-top:5px;letter-spacing:.4px;">{est}</div>'
            f'<div style="font-size:11px;color:{MUT};margin-top:4px;">{detalle}</div></div>')


def construir_dashboard():
    P = PARAMS
    v, ctl = REPLAY["vista"], REPLAY["control_state"]
    en, act = ctl["energia"], ctl["actuadores"]
    estado = ctl["estado"]
    pill = {"ALERTA": ("#FEE2E2", "#991B1B", "⚠️ ALERTA"), "ACTUANDO": ("#FEF3C7", "#92400E", "⚙️ ACTUANDO"),
            "ESTABLE": ("#DCFCE7", "#166534", "✅ ESTABLE")}[estado]
    pct = max(0.0, min(100.0, en["bateria_pct"]))
    bcol = "#15803D" if pct >= 40 else ("#B45309" if pct >= 22 else "#B91C1C")

    kpis = "".join([
        kpi("Estado del sistema", f'<span style="background:{pill[0]};color:{pill[1]};border-radius:999px;padding:3px 12px;font-size:16px;">{pill[2]}</span>', "último ciclo evaluado"),
        kpi("Batería", f"{pct:.1f} %", f"{en['bateria_final_wh']:.0f} / {P['bateria_cap_wh']:.0f} Wh", bcol),
        kpi("Radiación solar", fmt(v.get("rad"), 0, " W/m²"), f"nubosidad {fmt(v.get('nubosidad'), 0, ' %')} · {'☀️ día' if (v.get('es_dia') or 0) >= 0.5 else '🌙 noche'}"),
        kpi("Energía del ciclo", f"{en['energia_generada_wh']:.1f} Wh", f"panel {en['potencia_generada_w']:.0f} W × {P['duracion_ciclo_h']} h"),
    ])

    cultivo = "".join([
        gauge("🌡️", "Temperatura del aire", v.get("temp"), 10, 45, "°C", 1, (P["t_baja"], P["t_alta"]), (P["t_baja"], P["t_alta"])),
        gauge("💧", "Humedad del aire", v.get("hum_amb"), 0, 100, "%", 0, (0, P["hum_aire_alta"]), (P["hum_aire_alta"],)),
        gauge("🌱", "Humedad del suelo", v.get("suelo"), 0, 100, "%", 1, (P["umbral_suelo"], 100), (P["umbral_suelo"],)),
        gauge("🌍", "Temperatura del suelo", v.get("temp_suelo"), 10, 45, "°C", 1, (0, P["temp_suelo_alta"]), (P["temp_suelo_alta"],)),
        gauge("📶", "Señal Wi-Fi", v.get("wifi"), -100, 0, "", 0, (P["wifi_dbm_min"], 0), (P["wifi_dbm_min"],))
        if (v.get("wifi") is None or es_nan(v.get("wifi")) or v.get("wifi") <= 0)
        else gauge("📶", "Señal Wi-Fi", v.get("wifi"), 0, 100, "", 0, (P["wifi_pct_min"], 100), (P["wifi_pct_min"],)),
        gauge("🌧️", "Precipitación (Open-Meteo)", v.get("precip"), 0, 10, "mm/h", 2, (0, P["umbral_lluvia_mm"]), (P["umbral_lluvia_mm"],)),
    ])
    tarjetas_c = f'<div style="background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:14px;">' \
                 f'<div style="font-weight:800;color:{INK};">🏡 Cultivo y clima <span style="font-weight:400;color:{MUT};font-size:12px;">(la línea negra marca el umbral)</span></div>{cultivo}</div>'

    demanda = en["energia_requerida_wh"]
    if demanda <= 0:
        regla = f'<span style="color:{MUT};">Sin demanda: ningún actuador necesita energía.</span>'
    else:
        ok = not ctl["bloqueados"]
        regla = (f'<b style="color:{"#166534" if ok else "#991B1B"};">Disponible {en["energia_disponible_wh"]:.1f} Wh '
                 f'{"≥" if ok else "<"} Requerida {demanda:.1f} Wh → {"se activa" if ok else "SE BLOQUEA"}</b>')
    flecha = f'<div style="align-self:center;font-size:26px;color:{MUT};">➜</div>'
    caja = lambda t, a, b: (f'<div style="flex:1 1 120px;background:#F8FAFC;border:1px solid {LINE};border-radius:12px;padding:10px;text-align:center;">'
                            f'<div style="font-size:12px;color:{MUT};">{t}</div><div style="font-size:18px;font-weight:800;color:{INK};">{a}</div>'
                            f'<div style="font-size:12px;color:{MUT};">{b}</div></div>')
    gen_wh, gen_w = f"{en['energia_generada_wh']:.1f} Wh", f"{en['potencia_generada_w']:.0f} W"
    bat_ini, bat_res = f"{en['bateria_inicial_wh']:.0f} Wh", f"reserva {en['reserva_wh']:.0f} Wh"
    dem_txt = f"{demanda:.1f} Wh"
    res_pct = P["reserva_pct"] * 100
    energia = (f'<div style="background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:14px;">'
               f'<div style="font-weight:800;color:{INK};margin-bottom:10px;">⚡ Balance de energía del ciclo</div>'
               f'<div style="display:flex;gap:8px;flex-wrap:wrap;">'
               f'{caja("☀️ Panel", gen_wh, gen_w)}{flecha}{caja("🔋 Batería", bat_ini, bat_res)}{flecha}'
               f'{caja("⚙️ Actuadores", dem_txt, "energía requerida")}</div>'
               f'<div style="margin-top:10px;font-size:13px;">{regla}</div>'
               f'<div style="position:relative;background:#E2E8F0;border-radius:999px;height:14px;margin-top:12px;">'
               f'<div style="width:{pct:.0f}%;background:{bcol};height:14px;border-radius:999px;"></div>'
               f'<div style="position:absolute;left:{res_pct:.0f}%;top:-2px;width:2px;height:18px;background:{INK};"></div></div>'
               f'<div style="font-size:11px;color:{MUT};margin-top:4px;">Batería tras el ciclo: {pct:.1f} % · línea negra = reserva mínima ({res_pct:.0f} %)</div></div>')

    tarjetas = "".join(
        tarjeta_actuador(ic, nom, act[k], k in ctl["bloqueados"], ctl["necesidad"].get(k, False), f"{p:.0f} W × {h} h = {p * h:.0f} Wh")
        for k, nom, ic, p, h in lista_actuadores())
    actuadores = (f'<div style="font-weight:800;color:{INK};margin:4px 0 8px;">🛠️ Actuadores simulados</div>'
                  f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;">{tarjetas}</div>')

    al = "".join(f'<div style="margin:6px 0;padding:9px 11px;background:#FEE2E2;border:1px solid #DC2626;border-radius:8px;color:#7F1D1D;font-size:14px;font-weight:700;">🚨 {a}</div>' for a in ctl["alertas"])
    ac = "".join(f'<div style="margin:6px 0;padding:9px 11px;background:#DCFCE7;border:1px solid #16A34A;border-radius:8px;color:#14532D;font-size:14px;">{a}</div>' for a in ctl["acciones"])
    ev = "".join(f'<div style="margin:4px 0;color:#78350F;font-size:13px;">🔔 {e}</div>' for e in ctl["eventos"])
    decision = (f'<div style="background:#FFFFFF;border:1px solid {LINE};border-left:6px solid #1E3A5F;border-radius:12px;padding:12px 14px;margin-top:12px;">'
                f'<div style="font-weight:800;color:{INK};margin-bottom:6px;">🤖 Decisión del sistema</div>{al}{ac}{ev}</div>')

    return (f'<div style="font-family:Arial,Helvetica,sans-serif;color:{INK};">'
            f'<div style="display:flex;flex-wrap:wrap;gap:10px;">{kpis}</div>'
            f'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin:12px 0;">{tarjetas_c}{energia}</div>'
            f'{actuadores}{decision}</div>')


def construir_grafico_html():
    h = REPLAY["hist"][-60:]
    caja = f'background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:12px 14px;font-family:Arial,sans-serif;'
    if len(h) < 2:
        return (f'<div style="{caja}"><b style="color:{INK};">📈 Tendencia por ciclo</b>'
                f'<div style="color:{MUT};font-size:13px;margin-top:6px;">Se dibuja cuando haya al menos 2 ciclos evaluados.</div></div>')
    W, H, L, B, T = 640, 210, 40, 24, 10
    n = len(h)
    X = lambda i: L + (W - L - 10) * i / (n - 1)
    Y = lambda val: T + (H - T - B) * (1 - max(0, min(100, val)) / 100)

    def linea(key, color, escala=1.0):
        pts = " ".join(f"{X(i):.1f},{Y((0 if es_nan(p[key]) else p[key]) * escala):.1f}" for i, p in enumerate(h))
        return f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2.4"/>'

    grid = "".join(f'<line x1="{L}" y1="{Y(g):.1f}" x2="{W - 10}" y2="{Y(g):.1f}" stroke="#E2E8F0"/>'
                   f'<text x="{L - 6}" y="{Y(g) + 4:.1f}" font-size="10" text-anchor="end" fill="{MUT}">{g}</text>' for g in (0, 25, 50, 75, 100))
    umbrales = (f'<line x1="{L}" y1="{Y(PARAMS["umbral_suelo"]):.1f}" x2="{W - 10}" y2="{Y(PARAMS["umbral_suelo"]):.1f}" stroke="#15803D" stroke-dasharray="5 4"/>'
                f'<line x1="{L}" y1="{Y(PARAMS["reserva_pct"] * 100):.1f}" x2="{W - 10}" y2="{Y(PARAMS["reserva_pct"] * 100):.1f}" stroke="#1D4ED8" stroke-dasharray="5 4"/>')
    leyenda = (f'<span style="color:#1D4ED8;font-weight:700;">━ Batería %</span> &nbsp; <span style="color:#15803D;font-weight:700;">━ Humedad suelo %</span> &nbsp; '
               f'<span style="color:#B45309;font-weight:700;">━ Radiación (% de 1000 W/m²)</span> &nbsp; <span style="color:{MUT};">- - umbral de riego y reserva</span>')
    return (f'<div style="{caja}"><b style="color:{INK};">📈 Tendencia por ciclo (últimos {n})</b>'
            f'<svg viewBox="0 0 {W} {H}" style="width:100%;height:auto;margin-top:6px;">{grid}{umbrales}'
            f'{linea("rad", "#B45309", 0.1)}{linea("suelo", "#15803D")}{linea("bat", "#1D4ED8")}'
            f'<text x="{L}" y="{H - 6}" font-size="10" fill="{MUT}">ciclo {h[0]["ciclo"]}</text>'
            f'<text x="{W - 10}" y="{H - 6}" font-size="10" text-anchor="end" fill="{MUT}">ciclo {h[-1]["ciclo"]}</text></svg>'
            f'<div style="font-size:12px;">{leyenda}</div></div>')


def construir_log_html():
    est = {"alerta": ("🚨", "#FEE2E2", "#7F1D1D"), "evento": ("🔔", "#FEF3C7", "#78350F"), "ok": ("✅", "#DCFCE7", "#14532D")}
    filas = ""
    for tipo, hora, ciclo, origen, texto in reversed(REPLAY["log"][-15:]):
        ic, bg, c = est[tipo]
        filas += (f'<div style="background:{bg};color:{c};border-radius:8px;padding:6px 10px;margin:4px 0;font-size:13px;">'
                  f'{ic} <b>{hora}</b> · ciclo {ciclo} · {origen} — {texto}</div>')
    filas = filas or f'<div style="color:{MUT};font-size:13px;">Aún no hay eventos. Inicia el replay o ejecuta un ciclo manual.</div>'
    return (f'<div style="background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:12px 14px;font-family:Arial,sans-serif;">'
            f'<b style="color:{INK};">📋 Eventos y alertas (más reciente primero)</b>{filas}</div>')


def construir_barra_replay(velocidad, intervalo):
    regs = DATOS["registros"]
    actual, total = REPLAY["index"] + 1, len(regs)
    intervalo = max(1, int(intervalo))
    faltan = max(0, intervalo - REPLAY["seg_control"])
    avance_v = REPLAY["seg_control"] / intervalo * 100
    ts = REPLAY["vista"].get("created_at")
    fecha = "ciclo manual (sin timestamp histórico)" if ts is None else ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    estado = ("▶️ EN EJECUCIÓN", "#DCFCE7", "#166534") if REPLAY["playing"] else ("⏸️ PAUSADO", "#F1F5F9", "#334155")
    fase = ("🧠⚡ CONTROL Y ACTUACIÓN", "#FEF3C7", "#92400E") if REPLAY["fase"] == "control" else ("📡 TELEMETRÍA", "#E0F2FE", "#075985")
    badge = lambda t: f'<span style="background:{t[1]};color:{t[2]};border-radius:999px;padding:4px 12px;font-size:12px;font-weight:800;">{t[0]}</span>'
    return (f'<div style="background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:12px 14px;font-family:Arial,sans-serif;">'
            f'<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">{badge(estado)}{badge(fase)}'
            f'<span style="color:{MUT};font-size:12px;">Registro <b style="color:{INK};">{actual:,}/{total:,}</b> · {fecha} · 1 registro cada {int(velocidad)} s</span></div>'
            f'<div style="background:#E2E8F0;border-radius:999px;height:8px;margin-top:10px;"><div style="width:{actual / total * 100:.1f}%;background:#1D4ED8;height:8px;border-radius:999px;"></div></div>'
            f'<div style="display:flex;justify-content:space-between;font-size:12px;color:{MUT};margin-top:8px;"><span>Próxima decisión del borde en <b style="color:{INK};">{faltan} s</b></span><span>{REPLAY["mensaje"]}</span></div>'
            f'<div style="background:#E2E8F0;border-radius:999px;height:6px;margin-top:4px;"><div style="width:{avance_v:.0f}%;background:#B45309;height:6px;border-radius:999px;"></div></div></div>')


def construir_explicacion():
    d = REPLAY["decision"]
    caja = f'background:#FFFFFF;border:1px solid {LINE};border-radius:14px;padding:12px 14px;font-family:Arial,sans-serif;font-size:14px;color:{INK};line-height:1.6;'
    if d is None:
        return (f'<div style="{caja}"><b>🧩 ¿Qué está ocurriendo?</b><br>Aún no hay un ciclo evaluado. Pulsa <b>Iniciar</b>, <b>Avanzar un ciclo</b> o ejecuta un escenario manual.<br>'
                f'<span style="color:{MUT};">Sensor mide · ThingSpeak almacena · Open-Meteo aporta la radiación · el borde decide con <i>energía disponible ≥ requerida</i> · el actuador ejecuta.</span></div>')
    e, en = d["entradas"], d["energia"]
    return (f'<div style="{caja}"><b>🧩 Último ciclo: dato → condición → evento → respuesta</b><br>'
            f'<b>Dato capturado:</b> suelo {e["suelo"]:.1f} %, temperatura {e["temp"]:.1f} °C, radiación {e["rad"]:.0f} W/m².<br>'
            f'<b>Condición detectada:</b> {"; ".join(d["eventos"]) or "ninguna fuera de lo normal"}.<br>'
            f'<b>Energía:</b> generada {en["energia_generada_wh"]:.1f} Wh · disponible {en["energia_disponible_wh"]:.1f} Wh · requerida {en["energia_requerida_wh"]:.1f} Wh.<br>'
            f'<b>Respuesta del sistema:</b> {" ".join(d["alertas"] + d["acciones"])}</div>')


def construir_persistencia_html():
    n = len(SESION["ciclos_control"])
    filas = len(DATOS["df"]) if DATOS["df"] is not None else 0
    return tabla_html(["Archivo", "Contenido", "Estado"],
                      [[f"<code>{ARCHIVO_CSV.name}</code>", "Dataset limpio ThingSpeak + Open-Meteo (con la ruta de cada registro)", f"<b>{filas:,}</b> registros"],
                       [f"<code>{ARCHIVO_SESION.name}</code>", "Un registro por ciclo de control: promedios, energía, actuadores, eventos y alertas", f"<b>{n:,}</b> ciclos"]]) + \
           f'<div style="font-size:12px;color:{MUT};margin-top:8px;">Las lecturas individuales del replay no se guardan una por una: solo se persiste cada ciclo de control.</div>'


def construir_concurrencia_html():
    with LOCK_NUBE:
        nube = dict(ESTADO_NUBE)
    rep, pend, pid = dict(ESTADO_REPLAY), COLA_NUBE.qsize(), os.getpid()
    col = {"EJECUTÁNDOSE": ("#DCFCE7", "#166534"), "ESPERANDO RED": ("#FEF3C7", "#92400E"),
           "ESPERANDO DATOS": ("#E0F2FE", "#075985"), "ESPERANDO PRÓXIMO TICK": ("#E0F2FE", "#075985"),
           "FINALIZADO": ("#EDE9FE", "#5B21B6"), "PAUSADO": ("#F1F5F9", "#334155"),
           "PREPARADO": ("#F1F5F9", "#334155"), "INICIANDO": ("#F1F5F9", "#334155")}

    def caja(titulo, estado, detalle, ident):
        bg, c = col.get(estado, ("#F1F5F9", INK))
        return (f'<div style="flex:1 1 280px;background:#FFFFFF;border:1px solid {LINE};border-radius:12px;padding:12px;">'
                f'<div style="font-weight:800;color:{INK};">{titulo}</div>'
                f'<span style="display:inline-block;background:{bg};color:{c};font-weight:700;padding:4px 10px;border-radius:999px;margin:7px 0;">{estado}</span>'
                f'<div style="color:#1E293B;font-size:13px;">{detalle}</div><div style="color:{MUT};font-size:11px;margin-top:5px;">{ident}</div></div>')
    id_rep = f"hilo {rep.get('hilo_id') or '-'}"
    id_nube = f"hilo dedicado {nube.get('hilo_id') or '-'}"
    c1_ = caja("Replay y control local", rep["estado"], rep["detalle"], id_rep)
    c2_ = caja("Hilo de comunicación con la nube", nube["estado"], nube["detalle"], id_nube)
    return (f'<div style="font-family:Arial,sans-serif;"><div style="color:{MUT};font-size:13px;margin-bottom:8px;">Un solo proceso Python (PID {pid}). '
            f'El control local y el envío a la nube avanzan durante el mismo periodo: es <b>concurrencia</b>, no paralelismo.</div>'
            f'<div style="display:flex;flex-wrap:wrap;gap:10px;">{c1_}{c2_}</div>'
            f'<div style="margin-top:10px;background:#F8FAFC;border:1px solid {LINE};border-radius:10px;padding:9px;font-size:13px;color:{INK};">'
            f'<b>Cola:</b> {pend} pendiente(s) · <b>Confirmados:</b> {nube["enviados"]} · <b>Última confirmación:</b> {nube["ultimo_envio"]}</div></div>')


def construir_salida(velocidad=1, intervalo=15):
    return (construir_barra_replay(velocidad, intervalo), construir_dashboard(), construir_grafico_html(),
            construir_log_html(), construir_explicacion(), construir_persistencia_html(), construir_concurrencia_html())


# ============================================================
#  EVENTOS DE LA INTERFAZ
# ============================================================
def iniciar(velocidad, intervalo, demora):
    with LOCK_REPLAY:
        REPLAY["playing"] = True
        REPLAY["mensaje"] = "Replay iniciado."
        ESTADO_REPLAY.update(estado="ESPERANDO PRÓXIMO TICK", detalle="Replay iniciado; espera el siguiente pulso.", hilo_id=threading.get_ident())
        return construir_salida(velocidad, intervalo)


def pausar(velocidad, intervalo, demora):
    with LOCK_REPLAY:
        REPLAY["playing"] = False
        REPLAY["mensaje"] = "Replay en pausa."
        ESTADO_REPLAY.update(estado="PAUSADO", detalle="El usuario pausó la adquisición; el hilo de nube sigue por su cuenta.", hilo_id=threading.get_ident())
        return construir_salida(velocidad, intervalo)


def reiniciar(velocidad, intervalo, demora, bateria_inicial):
    with LOCK_REPLAY:
        reiniciar_estado(bateria_inicial)
        ESTADO_REPLAY.update(estado="PREPARADO", detalle="Replay reiniciado; listo para comenzar.", hilo_id=threading.get_ident())
        return construir_salida(velocidad, intervalo)


def avanzar_un_ciclo(velocidad, intervalo, demora):
    with LOCK_REPLAY:
        REPLAY["playing"] = False
        k = max(1, int(intervalo) // max(1, int(velocidad)))
        for _ in range(k):
            if not avanzar_registro():
                break
        ejecutar_ciclo_replay(int(intervalo), float(demora))
        REPLAY["mensaje"] = f"Ciclo avanzado manualmente ({k} registros)."
        ESTADO_REPLAY.update(estado="PAUSADO", detalle=REPLAY["mensaje"], hilo_id=threading.get_ident())
        return construir_salida(velocidad, intervalo)


def tick_replay(velocidad, intervalo, demora):
    velocidad, intervalo, demora = max(1, int(velocidad)), max(1, int(intervalo)), max(1, float(demora))
    with LOCK_REPLAY:
        ESTADO_REPLAY.update(estado="EJECUTÁNDOSE", detalle="Atendiendo el pulso: lectura, acumulación y posible decisión local.",
                             hilo_id=threading.get_ident())
        if not REPLAY["playing"]:
            ESTADO_REPLAY.update(estado="PAUSADO", detalle="El replay está pausado; la comunicación puede terminar independientemente.")
            return construir_salida(velocidad, intervalo)
        if REPLAY["fase_ticks"] > 0:
            REPLAY["fase_ticks"] -= 1
            REPLAY["fase"] = "control"
        else:
            REPLAY["fase"] = "telemetria"
        REPLAY["seg_control"] += 1
        REPLAY["seg_emision"] += 1
        msg = "Sin registro nuevo en este segundo."
        if REPLAY["seg_emision"] >= velocidad:
            REPLAY["seg_emision"] = 0
            if avanzar_registro():
                msg = f"Se reprodujo el registro {REPLAY['index'] + 1:,}."
            else:
                REPLAY["playing"] = False
                msg = "Fin de los registros cargados."
        if REPLAY["seg_control"] >= intervalo or (not REPLAY["playing"] and REPLAY["buffer"]):
            ejecutar_ciclo_replay(intervalo, demora)
            msg += f" ☁️ Paquete {len(SESION['ciclos_control'])} a la cola · 🧠⚡ el borde evaluó."
        REPLAY["mensaje"] = msg
        ESTADO_REPLAY.update(estado="ESPERANDO PRÓXIMO TICK", detalle=msg)
        return construir_salida(velocidad, intervalo)


# ============================================================
#  INTERFAZ GRADIO — 5 pestañas
# ============================================================
WEBHOOK_N8N = os.environ.get("WEBHOOK_N8N", "")
SESSION_ID_N8N = str(uuid.uuid4())

def consultarAgenteN8n(mensajeUsuario, historial):
    payload = {
        "chatInput": mensajeUsuario,
        "sessionId": SESSION_ID_N8N
    }
    try:
        r = requests.post(WEBHOOK_N8N, json=payload, timeout=60)
        r.raise_for_status()
        datos = r.json()
        if isinstance(datos, list) and len(datos) > 0:
            item = datos[0]
            return item.get("respuesta", item.get("output", str(item)))
        elif isinstance(datos, dict):
            return datos.get("respuesta", datos.get("output", str(datos)))
        return str(datos)
    except Exception as error:
        return f"Error de conexión con n8n: {error}"

CSS = r"""
.gradio-container { background:#EEF2F7 !important; font-family:Arial,Helvetica,sans-serif !important; max-width:1400px !important; }
.gradio-container .tab-nav button, .gradio-container button[role="tab"] { font-weight:800 !important; font-size:15px !important; }
.paso { background:#FFFFFF; border:1px solid #CBD5E1; border-left:6px solid #1D4ED8; border-radius:10px; padding:10px 14px; margin:6px 0 10px; color:#0F172A; font-size:14px; line-height:1.5; }
"""

def paso(texto):
    return f'<div class="paso">{texto}</div>'

BANNER = """
<div style="background:linear-gradient(135deg,#0F2A4A,#1E4F7A);border-radius:16px;padding:18px 22px;font-family:Arial,sans-serif;">
  <div style="font-size:11px;font-weight:800;letter-spacing:1.6px;color:#93C5FD;">GEMELO DIGITAL · INVERNADERO AUTÓNOMO OFF-GRID</div>
  <div style="font-size:25px;font-weight:900;color:#FFFFFF;margin:4px 0;">🌱 ¿Alcanza la energía solar para cuidar el cultivo? + 🤖 Chat Agéntico n8n</div>
  <div style="font-size:14px;color:#E2E8F0;line-height:1.55;">Datos reales de ThingSpeak + radiación solar de Open-Meteo →
  el borde decide <b style="color:#FFFFFF;">solo si la energía disponible alcanza</b> → Persistencia consultable vía Agente IA en n8n.</div>
</div>"""

cargar_datos(N_REGISTROS)
reiniciar_estado(50)

_G6 = int(gr.__version__.split(".")[0]) >= 6
_ESTILO = dict(theme=gr.themes.Soft(primary_hue="blue", neutral_hue="slate"), css=CSS)

with gr.Blocks(title="Gemelo digital — Invernadero off-grid", **({} if _G6 else _ESTILO)) as demo:

    gr.HTML(BANNER)

    with gr.Tabs():

        # ---------------------------------------------------- 1. DATOS
        with gr.Tab("1️⃣ Datos"):
            gr.HTML(paso("<b>Qué puedes cambiar aquí:</b> cuántos registros descargar de ThingSpeak."))
            with gr.Row():
                n_registros = gr.Slider(100, MAX_REGISTROS_THINGSPEAK, value=N_REGISTROS, step=100,
                                        label="Cantidad de registros a descargar", scale=3)
                btn_cargar = gr.Button("📥 Descargar y sincronizar", variant="primary", scale=1)
            resumen_datos = gr.HTML(construir_resumen_datos())
            with gr.Accordion("🧾 Notas del procesamiento", open=False):
                notas_html = gr.HTML(construir_notas_html())
            with gr.Accordion("🔎 Detalle: campos, muestra y ruta Open-Meteo", open=False):
                detalle_html = gr.HTML(construir_detalle_html())

        # ---------------------------------------------------- 2. PARÁMETROS
        with gr.Tab("2️⃣ Parámetros"):
            gr.HTML(paso("<b>Qué puedes cambiar aquí:</b> el panel solar, la batería, los umbrales y la potencia de cada actuador."))
            param_comps = []
            for grupo, items in PARAM_UI.items():
                with gr.Accordion(grupo, open=True):
                    mitad = (len(items) + 1) // 2
                    with gr.Row():
                        for bloque in (items[:mitad], items[mitad:]):
                            with gr.Column():
                                for (k, etiqueta, mn, mx, st) in bloque:
                                    param_comps.append(gr.Slider(mn, mx, value=PARAMS[k], step=st, label=etiqueta))
            with gr.Row():
                btn_aplicar = gr.Button("✅ Aplicar y reiniciar simulación", variant="primary")
                btn_restaurar = gr.Button("↩️ Restaurar valores")
            estado_params = gr.HTML()

        # ---------------------------------------------------- 3. SIMULACIÓN
        with gr.Tab("3️⃣ Simulación"):
            with gr.Row(equal_height=False):
                with gr.Column(scale=1, min_width=300):
                    gr.HTML(paso("<b>Controles.</b> Pulsa ▶️ para el replay en vivo, o ⏭ para procesar un ciclo completo al instante."))
                    velocidad = gr.Radio([1, 2, 5], value=1, label="Segundos por registro (replay)")
                    intervalo_control = gr.Radio([15, 20, 25, 30], value=15, label="Cada cuántos segundos decide el borde")
                    demora_red = gr.Radio([3, 5, 8], value=5, label="Demora simulada de la nube (s)")
                    bateria_inicial = gr.Slider(0, 100, value=50, step=5, label="🔋 Batería inicial (%) · se aplica al reiniciar")
                    with gr.Row():
                        btn_iniciar = gr.Button("▶️ Iniciar", variant="primary")
                        btn_pausar = gr.Button("⏸️ Pausar")
                    with gr.Row():
                        btn_paso = gr.Button("⏭ Avanzar un ciclo")
                        btn_reiniciar = gr.Button("🔄 Reiniciar")
                    with gr.Accordion("🧪 Escenarios y simulador manual", open=False):
                        with gr.Row():
                            btn_normal = gr.Button("✅ Normal")
                            btn_evento = gr.Button("🌵 Evento")
                            btn_critico = gr.Button("🚨 Crítico")
                        m_temp = gr.Slider(10, 45, value=25, step=1, label="🌡️ Temperatura del aire (°C)")
                        m_hum = gr.Slider(20, 100, value=65, step=1, label="💧 Humedad del aire (%)")
                        m_suelo = gr.Slider(0, 100, value=55, step=1, label="🌱 Humedad del suelo (%)")
                        m_tsuelo = gr.Slider(10, 45, value=24, step=1, label="🌍 Temperatura del suelo (°C)")
                        m_wifi = gr.Slider(-100, 0, value=-60, step=1, label="📶 Señal Wi-Fi (dBm)")
                        m_rad = gr.Slider(0, 1100, value=650, step=10, label="☀️ Radiación solar (W/m²)")
                        m_precip = gr.Slider(0, 10, value=0, step=0.1, label="🌧️ Precipitación (mm/h)")
                        m_dia = gr.Checkbox(value=True, label="Es de día")
                        m_bat = gr.Slider(0, 100, value=70, step=5, label="🔋 Batería al inicio del ciclo (%)")
                        btn_manual = gr.Button("▶️ Ejecutar ciclo con estos valores")
                with gr.Column(scale=3, min_width=620):
                    barra_replay = gr.HTML(construir_barra_replay(1, 15))
                    dashboard = gr.HTML(construir_dashboard())
                    grafico = gr.HTML(construir_grafico_html())
                    log_html = gr.HTML(construir_log_html())
                    explicacion = gr.HTML(construir_explicacion())

        # ---------------------------------------------------- 4. PERSISTENCIA
        with gr.Tab("4️⃣ Persistencia y concurrencia"):
            gr.HTML(paso("<b>Evidencias verificables.</b> Descarga los archivos y ábrelos fuera del panel."))
            persist_html = gr.HTML(construir_persistencia_html())
            with gr.Row():
                with gr.Column():
                    btn_json = gr.Button("💾 Preparar JSON de ciclos")
                    archivo_json = gr.File(label="telemetria_procesada_invernadero.json", interactive=False)
                with gr.Column():
                    btn_csv = gr.Button("💾 Preparar CSV sincronizado")
                    archivo_csv = gr.File(label="datos_sincronizados.csv", interactive=False)
            gr.HTML('<div style="font-weight:800;color:#0F172A;margin:10px 0 4px;">🧵 Concurrencia: control local + envío a la nube</div>')
            concurrencia = gr.HTML(construir_concurrencia_html())

        # ---------------------------------------------------- 5. CHAT AGÉNTICO N8N
        with gr.Tab("5️⃣ Chat Agéntico (n8n)"):
            gr.HTML(paso(f"<b>Consulta inteligente sobre la capa de persistencia.</b> Conectado a <code>{WEBHOOK_N8N}</code> | Sesión activa: <code>{SESSION_ID_N8N[:8]}</code>"))
            gr.ChatInterface(
                fn=consultarAgenteN8n,
                examples=[
                    "¿Cuántos ciclos o registros hay almacenados y cuál fue el estado general?",
                    "¿En qué ciclos se presentaron alertas por energía insuficiente?",
                    "Compara la batería y humedad del suelo de los primeros ciclos que mencionaste."
                ]
            )

    salidas = [barra_replay, dashboard, grafico, log_html, explicacion, persist_html, concurrencia]
    ctrl = [velocidad, intervalo_control, demora_red]
    manual = [m_temp, m_hum, m_suelo, m_tsuelo, m_wifi, m_rad, m_precip, m_dia, m_bat]

    btn_iniciar.click(iniciar, ctrl, salidas)
    btn_pausar.click(pausar, ctrl, salidas)
    btn_paso.click(avanzar_un_ciclo, ctrl, salidas)
    btn_reiniciar.click(reiniciar, ctrl + [bateria_inicial], salidas)
    btn_manual.click(ejecutar_manual, manual + ctrl, salidas)

    def preset(clave, v, ic, dr):
        vals = PRESETS[clave]
        return (*vals, *ejecutar_manual(*vals, v, ic, dr, titulo=PRESET_TITULOS[clave]))

    btn_normal.click(lambda v, ic, dr: preset("normal", v, ic, dr), ctrl, manual + salidas)
    btn_evento.click(lambda v, ic, dr: preset("evento", v, ic, dr), ctrl, manual + salidas)
    btn_critico.click(lambda v, ic, dr: preset("critico", v, ic, dr), ctrl, manual + salidas)

    def recargar(n, v, ic, bat):
        try:
            with LOCK_REPLAY:
                cargar_datos(n)
                reiniciar_estado(bat)
        except Exception as e:
            raise gr.Error(f"No se pudieron cargar los datos: {e}")
        return (construir_resumen_datos(), construir_notas_html(), construir_detalle_html(),
                *construir_salida(v, ic))

    btn_cargar.click(recargar, [n_registros, velocidad, intervalo_control, bateria_inicial],
                     [resumen_datos, notas_html, detalle_html] + salidas)

    def estado_html(msg):
        return (f'<div style="background:#DCFCE7;border:1px solid #16A34A;border-radius:10px;padding:10px 14px;'
                f'color:#14532D;font-size:14px;">{msg}</div>')

    def aplicar_parametros(*args):
        vals, v, ic = args[:-2], args[-2], args[-1]
        with LOCK_REPLAY:
            for k, x in zip(PARAM_KEYS, vals):
                PARAMS[k] = float(x)
            reiniciar_estado()
        return (estado_html("✅ Parámetros aplicados. La simulación se reinició con los nuevos valores."),
                *construir_salida(v, ic))

    def restaurar_parametros(v, ic):
        with LOCK_REPLAY:
            PARAMS.update(PARAMS_DOC)
            reiniciar_estado()
        return (*[PARAMS[k] for k in PARAM_KEYS],
                estado_html("↩️ Se restauraron los valores."), *construir_salida(v, ic))

    btn_aplicar.click(aplicar_parametros, param_comps + [velocidad, intervalo_control], [estado_params] + salidas)
    btn_restaurar.click(restaurar_parametros, [velocidad, intervalo_control], param_comps + [estado_params] + salidas)

    btn_json.click(descargar_json_sesion, [], [archivo_json])
    btn_csv.click(descargar_csv_sincronizado, [], [archivo_csv])

    reloj = gr.Timer(value=1, active=True)
    reloj.tick(tick_replay, ctrl, salidas)

    demo.load(lambda v, ic: construir_salida(v, ic), [velocidad, intervalo_control], salidas)
    demo.load(None, None, None, js="() => { document.body.classList.remove('dark'); }")

demo.launch(
    server_name="0.0.0.0",
    server_port=int(os.environ.get("PORT", 7860)),
    **(_ESTILO if _G6 else {})
)
