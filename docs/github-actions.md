# GitHub Actions en este repositorio

## Workflows incluidos

### 1. `daily-train-and-publish.yml`
Se ejecuta a diario y hace:
1. instala dependencias,
2. reconstruye SQLite desde `data/history/`,
3. ejecuta `scripts/daily_run.py`,
4. hace commit de:
   - `data/history/`
   - `data/latest/`
   - `public/`
5. deja que GitHub Pages publique la web.

### 2. `recalibrate-hyperparams.yml`
Se lanza manualmente y opcionalmente en calendario.
Genera:
- CSVs de calibración,
- CSVs rolling,
- un JSON candidato en `config/candidates/`.

### 3. `deploy-pages.yml`
Publica `public/` con GitHub Pages usando la integración oficial de Pages por Actions.

## Qué tienes que hacer al subirlo a GitHub

1. Crear un repositorio público.
2. Subir el contenido de esta carpeta.
3. En GitHub, ir a:
   - **Settings → Pages**
   - elegir **Build and deployment → Source: GitHub Actions**
4. En **Settings → Actions → General**:
   - permitir acciones oficiales,
   - dejar permisos de lectura/escritura para `GITHUB_TOKEN`.
5. Hacer un primer push a `main`.
6. Lanzar manualmente `daily-train-and-publish.yml` desde la pestaña **Actions**.

## Secretos recomendados

De momento este repo no necesita secretos para funcionar como core del modelo.

Más adelante, cuando conectes el backend del bot, añadirás algo como:
- `BOT_BACKEND_WEBHOOK_URL`
- `BOT_BACKEND_SHARED_SECRET`

El workflow diario podrá hacer un `POST` al backend una vez publicadas las recomendaciones.

## Persistencia

GitHub Actions es efímero. Por eso el estado persistente se guarda en el propio repo como CSVs dentro de `data/history/`.
SQLite se reconstruye en cada ejecución.

## Buenas prácticas

- No programes el cron exactamente al comienzo de la hora.
- Mantén el workflow de recalibración separado del diario.
- No promociones hiperparámetros automáticamente al principio; usa PR o revisión manual.
