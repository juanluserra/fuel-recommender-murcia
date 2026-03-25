# Backend del bot y separación de responsabilidades

Este repositorio **no** es el backend del bot. Este repositorio es el **motor batch**:
- recolecta,
- entrena,
- publica resultados.

## Dónde va el backend del bot

La recomendación es desplegar el backend del bot en **Cloudflare Workers** y usar **Cloudflare D1** para guardar:
- `chat_id`
- municipio
- combustible
- preferencias de suscripción
- última notificación enviada

## Rol de GitHub en esa parte

GitHub sirve para:
- versionar el código del backend,
- desplegarlo mediante CI/CD si quieres,
- pero **no** para ejecutar el backend interactivo en tiempo real.

## Integración entre ambos

1. Este repo publica `public/data/index.json` y `public/data/<scope>__<fuel>.json`.
2. El backend del bot consume ese JSON o recibe una notificación desde el workflow diario.
3. El backend decide a quién enviar qué mensaje.

## Organización recomendada

Tienes dos opciones correctas:

### Opción A: repos separados
- repo 1: `fuel-price-predictor-core`
- repo 2: `fuel-price-predictor-bot`

### Opción B: monorepo
- `core/`
- `bot/`

Para empezar, yo recomiendo **repos separados**, porque separa muy bien:
- batch/modelado,
- backend interactivo.
