# CambioCUP Historical Data Extractor

Extractor de datos históricos de tasas de cambio del mercado informal cubano desde [cambiocup.com](https://www.cambiocup.com).

## Fuente de datos

### Fuente principal: API pública de CambioCUP

```
GET https://www.cambiocup.com/api/history?coin={COIN}&days={DAYS}
```

**¿Por qué esta fuente?** Tras analizar el código fuente de cambiocup.com:

1. **CambioCUP** ejecuta un **cron** cada ~10 minutos que consulta la API de QvaPay P2P (`api.qvapay.com/p2p/completed_pairs_average?coin=X`), calcula `(average_buy + average_sell) / 2` y lo almacena en una tabla `exchange` de Supabase.

2. El endpoint `/api/history` expone públicamente los datos históricos de esa tabla. Acepta `coin` (CUP, MLC, CLASICA, ETECSA, TROPICAL) y `days` (días hacia atrás desde ahora).

3. **Limitación**: Supabase tiene un límite por defecto de **1000 filas por query**. El extractor resuelve esto con una estrategia de **ventana deslizante** (sliding window pagination): solicita ventanas de tiempo progresivamente más cercanas al presente, con deduplicación por timestamp.

4. La **API de QvaPay** (`api.qvapay.com`) solo devuelve datos del momento actual (sin parámetro de fecha); no tiene histórico accesible. Por eso el histórico solo puede obtenerse de CambioCUP, que lo ha estado acumulando en su base de datos.

### Fuente complementaria: QvaPay P2P Snapshot

Opcionalmente, el extractor captura un **snapshot actual** de QvaPay que incluye las **ofertas individuales** (los valores reales de cada transacción P2P), no solo el promedio. Esto es útil para análisis de distribución de precios.

## Profundidad del histórico

| Moneda | Desde | Hasta | Días | Registros brutos | Días con datos |
|--------|-------|-------|------|-------------------|----------------|
| **CUP** | 2023-11-01 | 2026-02-24 | 845 | 49,700 | 825 |
| **MLC** | 2023-11-01 | 2026-02-24 | 845 | 48,270 | 813 |
| **CLASICA** | 2025-07-16 | 2026-02-24 | 222 | 18,990 | 221 |
| **ETECSA** | 2025-11-18 | 2026-02-24 | 97 | 13,671 | 99 |
| **TROPICAL** | 2025-12-17 | 2026-02-24 | 69 | 9,328 | 70 |

> **Nota**: CUP y MLC tienen datos desde noviembre 2023. CLASICA, ETECSA y TROPICAL fueron añadidas a CambioCUP en fechas posteriores.

## Estructura de archivos generados

```
output/
├── raw_cup.csv              # Todos los registros (~cada 10 min) de CUP
├── raw_mlc.csv              # Idem para MLC
├── raw_clasica.csv          # Idem para CLASICA
├── raw_etecsa.csv           # Idem para ETECSA
├── raw_tropical.csv         # Idem para TROPICAL
├── raw_all_coins.csv        # Todos los anteriores combinados
├── daily_cup.csv            # Agregados diarios OHLC para CUP
├── daily_mlc.csv            # Idem para MLC
├── daily_clasica.csv        # Idem para CLASICA
├── daily_etecsa.csv         # Idem para ETECSA
├── daily_tropical.csv       # Idem para TROPICAL
├── daily_all_coins.csv      # Todos los diarios combinados
├── qvapay_snapshot.csv      # Snapshot actual de ofertas QvaPay (opcional)
├── extraction_summary.json  # Metadata de la extracción
└── *.parquet                # Versiones Parquet de cada CSV (opcional)
```

### Formato CSV crudo (`raw_*.csv`)

```csv
datetime_utc,timestamp,coin,value
2023-11-01 22:11:48+00:00,1698876708,CUP,260.000000
2023-11-01 22:49:11+00:00,1698878951,CUP,259.000000
...
```

- **`value`** = `(average_buy + average_sell) / 2` del P2P de QvaPay en ese instante
- Frecuencia: ~1 registro cada 10 minutos (~144/día)

### Formato CSV diario (`daily_*.csv`)

```csv
date,coin,open,high,low,close,mean,median,std,count
2023-11-01,CUP,260.000000,260.000000,250.000000,258.540000,257.442500,258.770000,3.397586,8
2023-11-02,CUP,258.705259,258.705259,258.384024,258.485778,258.631418,258.705259,0.109297,17
...
```

- **open/close**: primer/último valor del día
- **high/low**: máximo/mínimo del día
- **mean/median**: promedio y mediana de todos los registros del día
- **std**: desviación estándar intradía
- **count**: número de registros ese día

## Instalación y uso

### Requisitos

- Python 3.10+
- Conexión a internet

### Instalación

```bash
cd extractor/
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Extracción completa (todos los coins)

```bash
python cambiocup_extractor.py
```

### Con Parquet y snapshot de QvaPay

```bash
python cambiocup_extractor.py --parquet --qvapay
```

### Solo monedas específicas

```bash
python cambiocup_extractor.py --coins CUP MLC
```

### Directorio de salida personalizado

```bash
python cambiocup_extractor.py -o ./data/historico/ --parquet --verbose
```

### Opciones disponibles

```
--coins CUP MLC CLASICA ETECSA TROPICAL   Monedas a extraer (default: todas)
-o, --output DIR                            Directorio de salida (default: ./output)
--parquet                                   También generar archivos Parquet
--qvapay                                    Capturar snapshot actual de QvaPay P2P
-v, --verbose                               Logging detallado (debug)
```

## Recolección periódica (forward-looking)

Para construir histórico hacia adelante con mayor granularidad (incluyendo ofertas individuales de QvaPay):

```bash
# Una vez (para crontab):
python periodic_collector.py -o ./data/periodic/

# Continuo (daemon):
python periodic_collector.py --continuous --interval 600 -o ./data/periodic/
```

**Crontab** (cada 10 minutos):
```
*/10 * * * * /path/to/venv/bin/python /path/to/periodic_collector.py -o /path/to/data/
```

Este collector guarda:
- `periodic_rates.csv`: valor CambioCUP + promedios QvaPay para cada moneda
- `periodic_offers.csv`: cada oferta individual P2P de QvaPay en ese momento

## Notas técnicas

### Estrategia de paginación

El endpoint `/api/history` tiene un límite de **1000 filas** (default de Supabase). El extractor usa una técnica de **sliding window**:

1. Solicita `days=100000` → obtiene los 1000 registros más antiguos
2. Toma el timestamp del último registro
3. Calcula cuántos días atrás está ese timestamp desde ahora
4. Solicita `days=N` donde N = ese cálculo, obteniendo los siguientes 1000
5. Repite hasta recibir <1000 registros (= llegó al presente)
6. Deduplica por timestamp en cada iteración

Esto permite extraer **todo** el histórico a pesar del límite de 1000 filas.

### Cómo se calcula el "precio" en CambioCUP

```
precio = (average_buy + average_sell) / 2
```

Donde `average_buy` y `average_sell` son los promedios de las transacciones P2P completadas en QvaPay para cada par de monedas. Es decir, es un **precio mid-market** del mercado informal P2P.

### Coin IDs en Supabase

| coin_id | Moneda | QvaPay coin | Descripción |
|---------|--------|-------------|-------------|
| 1 | CUP | BANK_CUP | Peso Cubano (transferencia bancaria) |
| 2 | MLC | BANK_MLC | Moneda Libremente Convertible |
| 3 | CLASICA | CLASICA | Tarjeta Clásica |
| 4 | ETECSA | ETECSA | Saldo ETECSA (telefonía) |
| 5 | TROPICAL | BANDECPREPAGO | BANDEC Prepago (Tropical) |

## Licencia

Para uso académico (TFM). Los datos provienen de fuentes públicas (APIs abiertas).
