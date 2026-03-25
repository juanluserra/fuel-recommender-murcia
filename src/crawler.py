"""
crawler.py — Obtiene precios de carburantes desde la API REST del Ministerio
de Industria, Turismo y Comercio (MINETUR / MITECO).

Documentación de la API:
  https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/help
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime
from typing import Optional

import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src import config


class _LegacySSLAdapter(HTTPAdapter):
    """Adaptador que relaja las restricciones SSL para servidores antiguos
    (como la API del Ministerio) que cierran la conexión con EOF inesperado."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        # Rebaja el nivel de seguridad para aceptar TLS antiguo
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        # Desactiva verificación estricta de renegociación (compatible con Python 3.11+)
        ctx.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)

logger = logging.getLogger(__name__)

# ─── Constantes internas ──────────────────────────────────────────────────────
_SESSION_TIMEOUT = 15          # segundos
_MAX_RETRIES     = 3
_RETRY_BACKOFF   = 2.0        # factor de espera exponencial


class MinturAPIError(Exception):
    """Error genérico de la API del Ministerio."""


class GasolinerasCrawler:
    """
    Cliente de la API REST de Precios de Carburantes del MINETUR.

    Uso básico
    ----------
    >>> crawler = GasolinerasCrawler()
    >>> records = crawler.fetch_today()
    """

    _BASE = config.API_BASE_URL

    def __init__(
        self,
        municipality_name: str | None = None,
        province_name: str | None = None,
        municipality_id: str | None = None,
    ) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        adapter = _LegacySSLAdapter()
        self.session.mount("https://", adapter)
        self.municipality_name = municipality_name or config.DEFAULT_MUNICIPALITY_NAME
        self.province_name = province_name or config.DEFAULT_PROVINCE_NAME
        self._municipality_id: Optional[str] = municipality_id or config.DEFAULT_MUNICIPALITY_ID

    # ── Métodos públicos ──────────────────────────────────────────────────────

    def fetch_today(self) -> list[dict]:
        """
        Descarga los precios actuales de todas las gasolineras de Alcantarilla.

        Returns
        -------
        list[dict]
            Lista de registros normalizados listos para insertar en la BD.
        """
        target_date = date.today().isoformat()
        raw = self._fetch_raw_for_date(target_date)
        return self._parse_stations(raw, target_date)

    def fetch_by_date(self, target_date: str) -> list[dict]:
        """Descarga los precios de una fecha concreta en formato YYYY-MM-DD."""
        target_date = self._normalize_target_date(target_date)
        raw = self._fetch_raw_for_date(target_date)
        return self._parse_stations(raw, target_date)

    def fetch_municipalities(self) -> list[dict]:
        """Devuelve la lista completa de municipios con su ID."""
        data = self._get("/Listados/Municipios/")
        return data if isinstance(data, list) else []

    def find_municipality_id(
        self,
        name: str | None = None,
        province: str | None = None,
    ) -> Optional[str]:
        """
        Busca el ID de un municipio por nombre y provincia.

        Parameters
        ----------
        name:     Nombre del municipio (case-insensitive, soporta tildes).
        province: Nombre de la provincia para desambiguar.
        """
        name = name or self.municipality_name
        province = province or self.province_name
        municipalities = self.fetch_municipalities()
        name_norm = _normalize(name)
        prov_norm = _normalize(province)

        candidates = [
            m for m in municipalities
            if _normalize(m.get("Municipio", "")) == name_norm
        ]
        if not candidates:
            raise MinturAPIError(
                f"Municipio '{name}' no encontrado en la API."
            )

        # Filtrar por provincia si hay más de un candidato
        if len(candidates) > 1:
            candidates = [
                m for m in candidates
                if prov_norm in _normalize(m.get("Provincia", ""))
            ]

        if not candidates:
            raise MinturAPIError(
                f"Municipio '{name}' no encontrado en la provincia '{province}'."
            )

        mun_id = candidates[0].get("IDMunicipio")
        logger.info(
            "Municipio encontrado: %s (ID=%s)", candidates[0].get("Municipio"), mun_id
        )
        return str(mun_id)

    # ── Métodos privados ──────────────────────────────────────────────────────

    def _get_municipality_id(self) -> str:
        if self._municipality_id is None:
            self._municipality_id = self.find_municipality_id()
        return self._municipality_id

    def fetch_provinces(self) -> list[dict]:
        """Devuelve la lista de provincias con su ID."""
        data = self._get("/Listados/Provincias/")
        return data if isinstance(data, list) else []

    def find_province_id(self, name: str | None = None) -> Optional[str]:
        name = name or self.province_name
        provinces = self.fetch_provinces()
        name_norm = _normalize(name)
        for prov in provinces:
            if _normalize(prov.get("Provincia", "")) == name_norm:
                prov_id = prov.get("IDPovincia") or prov.get("IDProvincia")
                logger.info("Provincia encontrada: %s (ID=%s)", prov.get("Provincia"), prov_id)
                return str(prov_id)
        raise MinturAPIError(f"Provincia '{name}' no encontrada en la API.")

    def _get_stations_by_municipality(self, mun_id: str, target_date: Optional[str] = None) -> list[dict]:
        endpoint_date = self._api_date(target_date) if target_date else None
        if endpoint_date and target_date != date.today().isoformat():
            endpoint = f"/EstacionesTerrestresHist/FiltroMunicipio/{endpoint_date}/{mun_id}"
        else:
            endpoint = f"/EstacionesTerrestres/FiltroMunicipio/{mun_id}"
        data = self._get(endpoint)
        # La API envuelve los resultados en la clave "ListaEESSPrecio"
        if isinstance(data, dict):
            stations = data.get("ListaEESSPrecio", [])
            if data.get("ResultadoConsulta") is not None:
                logger.info("ResultadoConsulta municipio: %s", data.get("ResultadoConsulta"))
            if data.get("Nota"):
                logger.info("Nota API municipio: %s", data.get("Nota"))
        else:
            stations = data or []
        logger.info("Gasolineras obtenidas por municipio: %d", len(stations))
        return stations

    def _get_stations_by_province_filtered(self, province: str, municipality: str, target_date: Optional[str] = None) -> list[dict]:
        province_id = self.find_province_id(province)
        endpoint_date = self._api_date(target_date) if target_date else None
        if endpoint_date and target_date != date.today().isoformat():
            endpoint = f"/EstacionesTerrestresHist/FiltroProvincia/{endpoint_date}/{province_id}"
        else:
            endpoint = f"/EstacionesTerrestres/FiltroProvincia/{province_id}"
        data = self._get(endpoint)
        if isinstance(data, dict):
            stations = data.get("ListaEESSPrecio", [])
            if data.get("ResultadoConsulta") is not None:
                logger.info("ResultadoConsulta provincia: %s", data.get("ResultadoConsulta"))
            if data.get("Nota"):
                logger.info("Nota API provincia: %s", data.get("Nota"))
        else:
            stations = data or []

        target = _normalize(municipality)
        filtered = [s for s in stations if _normalize(s.get("Municipio", "")) == target]
        logger.info(
            "Gasolineras obtenidas por provincia: %d; tras filtrar por municipio '%s': %d",
            len(stations), municipality, len(filtered)
        )
        return filtered

    def _parse_stations(self, raw: list[dict], target_date: str) -> list[dict]:
        """
        Normaliza los registros crudos de la API al formato interno.

        La API devuelve los precios como cadenas con coma decimal (ej. "1,659").
        Los campos vacíos ("") se convierten a None.
        """
        records: list[dict] = []

        for station in raw:
            base = {
                "date":            target_date,
                "fetched_at":      datetime.utcnow().isoformat(),
                "station_id":      station.get("IDEESS", ""),
                "station_name":    station.get("Rótulo", "").strip(),
                "address":         station.get("Dirección", "").strip(),
                "municipality":    station.get("Municipio", "").strip(),
                "province":        station.get("Provincia", "").strip(),
                "latitude":        _parse_coord(station.get("Latitud", "")),
                "longitude":       _parse_coord(
                    station.get("Longitud (WGS84)", station.get("Longitud", ""))
                ),
                "schedule":        station.get("Horario", "").strip(),
            }

            # Añadir precio de cada carburante conocido
            all_fields = (
                config.FUEL_FIELDS["diesel"] + config.FUEL_FIELDS["gasolina"]
            )
            for field in all_fields:
                raw_val = station.get(field, "")
                base[_field_to_col(field)] = _parse_price(raw_val)

            records.append(base)

        return records

    def _fetch_raw_for_date(self, target_date: str) -> list[dict]:
        mun_id = self._get_municipality_id()
        raw = self._get_stations_by_municipality(mun_id, target_date=target_date)

        if not raw:
            logger.warning(
                "FiltroMunicipio devolvió 0 estaciones para ID=%s y fecha=%s. Probando resolución dinámica/fallback por provincia.",
                mun_id,
                target_date,
            )
            dynamic_id = self.find_municipality_id()
            if dynamic_id != mun_id:
                self._municipality_id = dynamic_id
                raw = self._get_stations_by_municipality(dynamic_id, target_date=target_date)

        if not raw:
            raw = self._get_stations_by_province_filtered(
                province=self.province_name,
                municipality=self.municipality_name,
                target_date=target_date,
            )

        return raw

    def _normalize_target_date(self, target_date: str) -> str:
        try:
            return datetime.strptime(target_date, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise MinturAPIError(
                f"Fecha inválida '{target_date}'. Usa el formato YYYY-MM-DD."
            ) from exc

    def _api_date(self, target_date: str) -> str:
        """Convierte YYYY-MM-DD a DD-MM-YYYY para los endpoints históricos."""
        dt = datetime.strptime(target_date, "%Y-%m-%d").date()
        return dt.strftime("%d-%m-%Y")

    def _get(self, endpoint: str) -> dict | list:
        url = self._BASE + endpoint
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=_SESSION_TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as exc:
                logger.warning("HTTP %s en %s (intento %d)", exc.response.status_code, url, attempt)
            except requests.exceptions.RequestException as exc:
                logger.warning("Error de red en %s (intento %d): %s", url, attempt, exc)

            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF ** attempt)

        raise MinturAPIError(
            f"No se pudo obtener {url} tras {_MAX_RETRIES} intentos."
        )


# ─── Utilidades de transformación ────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Minúsculas y sin tildes para comparar nombres."""
    replacements = str.maketrans("áéíóúàèìòùäëïöüñÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ",
                                  "aeiouaeiouaeiounAEIOUAEIOUAEIOUN")
    return text.translate(replacements).lower().strip()


def _parse_price(raw: str) -> Optional[float]:
    """'1,659' → 1.659  |  '' → None"""
    raw = raw.strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def _parse_coord(raw: str) -> Optional[float]:
    """Convierte coordenadas con coma decimal a float."""
    return _parse_price(raw)


def _field_to_col(field: str) -> str:
    """'Precio Gasoil A' → 'precio_gasoil_a'"""
    col = field.lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    return col.strip("_")
