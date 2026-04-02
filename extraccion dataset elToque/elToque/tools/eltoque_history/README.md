# elToque Historical Exchange Rates Exporter

## Descripción

Script CLI en Python para descargar el histórico diario de tasas de cambio del mercado informal cubano desde la API de **elToque TRMI** y exportarlo a CSV.

## Fuente de datos

- **API**: [Tasas de elTOQUE TRMI](https://tasas.eltoque.com) (v1)
- **Endpoint**: `GET /v1/trmi?date_from=...&date_to=...`
- **Documentación Swagger**: https://tasas.eltoque.com/static/swagger.json
- **Tipo de dato**: Medianas de ofertas de compra y venta en Telegram, WhatsApp y sitios de clasificados
- **Autenticación**: Bearer JWT token

## Fecha más antigua alcanzada

- **Desde**: 2021-01-01
- **Hasta**: presente (ayer)
- Las fechas anteriores a 2021-01-01 devuelven `tasas: {}` (sin datos).

## Monedas / divisas incluidas

| Código | Descripción |
|---|---|
| `USD` | Dólar estadounidense |
| `ECU` | Euro (EUR en contexto cubano) |
| `MLC` | Moneda Libremente Convertible |
| `USDT_TRC20` | Tether (USDT) en red TRON |
| `BTC` | Bitcoin |
| `TRX` | TRON |
| `BNB` | Binance Coin |

> **Nota**: No todas las monedas están disponibles todos los días. Los primeros días (enero 2021) solo tienen USD, ECU y USDT_TRC20. BNB aparece esporádicamente.

## Columnas del CSV

| Columna | Tipo | Descripción |
|---|---|---|
| `date` | string (YYYY-MM-DD) | Fecha del dato |
| `BNB` | float | Tasa BNB/CUP (mediana) |
| `BTC` | float | Tasa BTC/CUP (mediana) |
| `ECU` | float | Tasa EUR/CUP (mediana) |
| `MLC` | float | Tasa MLC/CUP (mediana) |
| `TRX` | float | Tasa TRX/CUP (mediana) |
| `USD` | float | Tasa USD/CUP (mediana) |
| `USDT_TRC20` | float | Tasa USDT/CUP (mediana) |
| `spread_USD_MLC` | float | Diferencia USD - MLC |
| `spread_USD_USDT` | float | Diferencia USD - USDT |

> Las tasas representan el precio en CUP (pesos cubanos) por unidad de moneda extranjera.  
> Celdas vacías indican que esa moneda no tuvo datos ese día.

## Regla de "diario"

- Cada fila = 1 día calendario.
- Se consulta el rango `00:00:01 → 23:59:01` de cada día.
- La API calcula la mediana de todas las ofertas de compra/venta en las últimas 24 horas dentro de ese rango.

## Limitaciones

| Limitación | Detalle |
|---|---|
| **Rate limit** | 1 petición por segundo (la API devuelve 429 si se excede) |
| **Ventana máxima** | 24 horas por petición (no se puede pedir rangos mayores) |
| **Sin buy/sell separado** | La API solo devuelve la mediana, no precios de compra y venta por separado |
| **Sin min/max/percentiles** | La API no expone estadísticas adicionales (solo la mediana) |
| **Sin número de ofertas** | No se informa cuántas ofertas componen la mediana |
| **Datos ausentes** | Algunos días/monedas no tienen datos (la API devuelve `{}`) |
| **Cloudflare** | La API está detrás de Cloudflare; requiere User-Agent válido |
| **Token expira** | El JWT actual expira el 2027-03-03 |

## Instalación

```bash
cd tools/eltoque_history
pip install -r requirements.txt
```

## Uso

```bash
# Descargar todo el histórico disponible
python export.py --out data/eltoque_history.csv

# Solo desde una fecha
python export.py --out data/eltoque_history.csv --start 2023-01-01

# Rango específico
python export.py --out data/eltoque_history.csv --start 2024-01-01 --end 2024-12-31

# Con token como variable de entorno
ELTOQUE_API_TOKEN=eyJ... python export.py --out data/eltoque_history.csv

# Con archivo de token explícito
python export.py --out data/eltoque_history.csv --token-file /ruta/al/token.txt
```

### Opciones CLI

| Argumento | Descripción | Default |
|---|---|---|
| `--out`, `-o` | Ruta del CSV de salida (requerido) | — |
| `--start`, `-s` | Fecha inicio YYYY-MM-DD | `2021-01-01` |
| `--end`, `-e` | Fecha fin YYYY-MM-DD | ayer |
| `--token-file` | Ruta al archivo con el token | auto-detect |
| `--delay` | Segundos entre peticiones | `1.2` |
| `--no-checkpoint` | Deshabilitar guardado incremental | `False` |

### Checkpointing

El script guarda progreso cada 50 días en un archivo `.eltoque_history_checkpoint.json`.  
Si se interrumpe, al re-ejecutar con los mismos argumentos continuará desde donde quedó.

## Tiempo estimado

- ~1900 días × 1.2 seg/día ≈ **38 minutos** para el histórico completo.

## Licencia

Uso interno. Los datos son propiedad de [elTOQUE](https://eltoque.com). Debe referenciarse elTOQUE como fuente.
