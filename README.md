# Fuel Price Predictor

Repositorio preparado para GitHub como **core del entrenamiento diario** del modelo de recomendación de repostaje.

Objetivo del proyecto:
- mantener histórico público y reproducible de precios por municipio,
- reentrenar diariamente el modelo con unos hiperparámetros de producción fijos,
- publicar una recomendación diaria (`buy_now` / `wait`) por municipio y combustible,
- recalibrar hiperparámetros solo de forma ocasional.

## Estado de esta versión

Esta estructura está pensada para:
- empezar con **Alcantarilla (Murcia)**,
- conservar el histórico en **CSV versionado** para que GitHub Actions pueda reconstruir SQLite en cada ejecución,
- publicar salidas diarias en `public/` y `data/latest/`,
- dejar la recalibración rolling separada del flujo diario.

## Estructura

```text
FuelPrice-Predictor/
├── .github/workflows/
│   ├── daily-train-and-publish.yml
│   ├── recalibrate-hyperparams.yml
│   └── deploy-pages.yml
├── config/
│   ├── scopes.json
│   ├── production_models.json
│   ├── search_space.json
│   └── promotion_policy.json
├── data/
│   ├── history/
│   │   └── alcantarilla_murcia/
│   │       ├── stations.csv
│   │       ├── prices.csv
│   │       └── fetch_log.csv
│   ├── latest/
│   └── experiments/
├── docs/
│   ├── github-actions.md
│   └── backend-architecture.md
├── models/
├── notebooks/
├── public/
│   ├── data/
│   └── index.html
├── reports/
│   └── experiments/
├── scripts/
│   ├── rebuild_sqlite_from_history.py
│   ├── export_history_from_sqlite.py
│   ├── collect_history.py
│   ├── train_model_a.py
│   ├── daily_run.py
│   ├── recalibrate_hyperparams.py
│   └── select_production_candidate.py
└── src/
    ├── calibration.py
    ├── config.py
    ├── crawler.py
    ├── data_collection.py
    ├── database.py
    ├── dataset_builder.py
    ├── history_store.py
    ├── model_a.py
    ├── publication.py
    └── repo_config.py
```

## Filosofía operativa

### Diario
- reconstruir SQLite local a partir de `data/history/<scope_id>/*.csv`,
- recolectar el día nuevo,
- exportar el histórico actualizado otra vez a CSV,
- reentrenar el modelo usando `config/production_models.json`,
- publicar `public/data/*.json` y `public/index.html`.

### Ocasional (trimestral / semestral / anual)
- ejecutar búsqueda de hiperparámetros con split simple + rolling,
- generar una **configuración candidata** en `config/candidates/`,
- revisar resultados,
- promocionar manualmente si merece la pena.

## Configuración de producción inicial

Se deja configurado como modelo operativo inicial:
- `scope_id = alcantarilla_murcia`
- `fuel_col = precio_gasoleo_a`
- `horizon_days = 3`
- `waiting_cost = 0.001`

Esta selección se basa en la tabla rolling actual incluida en `reports/experiments/`.

## Instalación local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Primer uso local

Reconstruir SQLite desde el histórico versionado:

```bash
python scripts/rebuild_sqlite_from_history.py --scope-id alcantarilla_murcia
```

Entrenar y generar salida diaria sin tocar la API:

```bash
python scripts/daily_run.py --scope-id alcantarilla_murcia --skip-collect --fuel-col precio_gasoleo_a
```

Recolectar una fecha concreta y actualizar histórico:

```bash
python scripts/daily_run.py --scope-id alcantarilla_murcia --fuel-col precio_gasoleo_a --target-date 2026-03-24
```

Recalibrar hiperparámetros:

```bash
python scripts/recalibrate_hyperparams.py \
  --scope-id alcantarilla_murcia \
  --fuel-col precio_gasoleo_a \
  --run-rolling \
  --show-progress
```

Promover una configuración candidata:

```bash
python scripts/select_production_candidate.py \
  --candidate config/candidates/alcantarilla_murcia__precio_gasoleo_a.json \
  --write
```

## Qué se publica cada día

- `data/latest/<scope>__<fuel>__latest.csv`: tabla por estación del último día.
- `public/data/<scope>__<fuel>.json`: payload público listo para frontend o bot.
- `public/data/index.json`: feed agregado.
- `public/index.html`: vista pública simple para GitHub Pages.

## Notas importantes

- `data/history/` es el estado persistente pensado para Git.
- `data/db/` es efímero y se reconstruye cuando haga falta.
- `models/` se usa como cache local del entrenamiento, no como almacenamiento histórico de producción.
- El backend interactivo del bot **no** está en este repo; este repo solo genera y publica las recomendaciones.

## Documentación adicional

- Ver `docs/github-actions.md` para la integración en GitHub.
- Ver `docs/backend-architecture.md` para la separación entre este repo y el backend del bot.
