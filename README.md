# ⚽ Data Warehouse Transfermarkt

DW con ETL completo sobre el dataset de [Transfermarkt](https://www.kaggle.com/datasets/davidcariboo/player-scores/data) cargado en un Data Warehouse PostgreSQL con esquema en estrella (constelación) siguiendo la metodología **Kimball**.

---

## 📁 Estructura del proyecto

```
DW-Transfermarkt/
│
├── Proyecto/
│   ├── Formato_csv/ 
│   │   ├── players.csv
│   │   ├── clubs.csv
│   │   ├── competitions.csv
│   │   ├── games.csv
│   │   ├── club_games.csv
│   │   ├── appearances.csv
│   │   ├── game_lineups.csv
│   │   ├── game_events.csv
│   │   ├── transfers.csv
│   │   └── player_valuations.csv
│   │
│   └── etl/
│       ├── config.py <--- Configuración centralizada (rutas, BD, parámetros)
│       ├── null_handler.py <--- Módulo Kimball de normalización de NULLs
│       ├── run_etl_full.py <--- Orquestador principal (ejecuta todo en orden)
│       ├── generate_dim_date.py
│       ├── etl_dim_competitions.py
│       ├── etl_dim_clubs.py
│       ├── etl_dim_players.py
│       ├── etl_dim_games.py
│       ├── etl_fact_games.py
│       ├── etl_fact_appearances.py
│       ├── etl_fact_game_events.py
│       ├── etl_fact_transfers.py
│       ├── etl_fact_player_valuations.py
│       ├── validate_dwh.py <--- Validación post-carga
│       ├── check_nulls_final.py <--- Auditoría de NULLs en PostgreSQL
│       ├── informe_filtraciones.py
│       ├── comparacion_filtrados_validos.py
│       ├── ejemplo_filtrado.py
|       └── consultas_olap.sql <--- Consultas OLAP 
├── ddl_dwh_schema.sql <--- DDL del esquema (ejecutar antes del ETL)
└── README.md
```
Nota: al clonarse el proyecto, los archivos .csv git no los descarga bien, por eso pasar esos archivos .xlsx a formato .csv y sustituirlos por los de la carpeta Formato_csv

---

## 🗄️ Modelo de datos

Esquema en **constelación estrella** con 5 dimensiones y 5 tablas de hechos:

| Tipo | Tabla | Descripción |
|------|-------|-------------|
| Dimensión | `dim_date` | Calendario generado (2000–2030) |
| Dimensión | `dim_competitions` | Ligas y competiciones |
| Dimensión | `dim_clubs` | Clubes de fútbol |
| Dimensión | `dim_players` | Jugadores |
| Dimensión | `dim_games` | Partidos como dimensión |
| Hechos | `fact_games` | Resultado y estadísticas por partido |
| Hechos | `fact_appearances` | Actuación individual de cada jugador en cada partido |
| Hechos | `fact_game_events` | Eventos minuto a minuto (goles, tarjetas, sustituciones) |
| Hechos | `fact_transfers` | Traspasos de jugadores |
| Hechos | `fact_player_valuations` | Historial de valor de mercado |

---

## 🐧 1. Instalación de PostgreSQL en Windows

1. Descargar el instalador oficial desde: **https://www.postgresql.org/download/windows/**
2. Ejecutar el `.exe` como administrador.
3. Durante la instalación:
   - Dejar el puerto por defecto: **5432**
   - Establecer una contraseña para el usuario `postgres` — **apúntala**, la necesitarás en `config.py`
   - Dejar marcado **Stack Builder** si aparece (se puede omitir al final)
4. Al finalizar, PostgreSQL queda instalado como servicio de Windows y arranca automáticamente con el sistema.

Para verificar que está corriendo, abrir el **Administrador de tareas → Servicios** y buscar `postgresql-x64-XX` con estado **En ejecución**, o ejecutar en PowerShell:

```powershell
Get-Service -Name postgresql*
```

Si el servicio estuviera detenido, iniciarlo con (PowerShell como administrador):

```powershell
Start-Service -Name "postgresql-x64-17"   # ajustar el número de versión
```

## 🐧 1. Instalación de PostgreSQL en Linux(Ubuntu)

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib -y

# Iniciar el servicio y habilitarlo para que arranque con el sistema
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Verificar que está corriendo
sudo systemctl status postgresql
```

---
## 🗃️ 2. Crear la base de datos en Windows

PostgreSQL instala `psql` en su carpeta `bin` (normalmente `C:\Program Files\PostgreSQL\17\bin`). Para poder usarlo desde cualquier terminal hay que añadir esa ruta al **PATH** del sistema:

1. Buscar **"Variables de entorno"** en el menú Inicio → **Editar las variables de entorno del sistema**
2. En **Variables del sistema** → seleccionar `Path` → **Editar** → **Nuevo**
3. Añadir: `C:\Program Files\PostgreSQL\17\bin` (ajustar el número de versión)
4. Aceptar y cerrar. Abrir una terminal nueva para que el cambio surta efecto.

Abrir **PowerShell** o **CMD** y conectarse:

```powershell
psql -U postgres
```

Introducir la contraseña establecida durante la instalación. Dentro del cliente `psql`, ejecutar:

```sql
CREATE DATABASE football_dwh;

-- Verificar que se creó
\l

-- Salir
\q
```

---

## 🗃️ 2. Crear la base de datos en Linux (Ubuntu)

```bash
# Entrar al cliente psql como superusuario postgres
sudo -u postgres psql
```

Dentro del cliente `psql`, ejecutar:

```sql
CREATE DATABASE football_dwh;

-- Verificar que se creó
\l

-- Salir
\q
```

---

## 🔑 3. Configurar la contraseña en `config.py`

Abrir el archivo `Proyecto/etl/config.py` y localizar el bloque `DB_CONFIG`:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'football_dwh',
    'user': 'postgres',
    'password': 'pass'  # ⚠️ CAMBIAR AQUÍ por tu contraseña real de PostgreSQL
}
```

Sustituir `'pass'` por la contraseña de tu usuario `postgres`. Si no recuerdas la contraseña o quieres establecer una nueva:

```bash
sudo -u postgres psql
ALTER USER postgres WITH PASSWORD 'tu_nueva_contraseña';
\q
```

> **Nota:** Si prefieres usar un usuario PostgreSQL propio en lugar del superusuario `postgres`, cámbia también el campo `'user'` y asegúrate de que ese usuario tenga permisos sobre `football_dwh`:
> ```sql
> CREATE USER alberto WITH PASSWORD 'tu_contraseña';
> GRANT ALL PRIVILEGES ON DATABASE football_dwh TO alberto;
> ```

---
## 🐍 4. Instalar Python y dependencias en Windows

### Instalar Python

1. Descargar Python desde: **https://www.python.org/downloads/windows/**
2. Ejecutar el instalador y marcar **obligatoriamente** la opción **"Add Python to PATH"** antes de pulsar Install.
3. Verificar la instalación abriendo una terminal nueva:

```powershell
python --version
pip --version
```

### Crear y activar el entorno virtual

Abrir **PowerShell** o **CMD**, navegar a la raíz del proyecto y ejecutar:

```powershell
# Crear el entorno virtual
python -m venv nombre_entorno_virtual

# Activar (CMD)
nombre_entorno_virtual\Scripts\activate.bat

# Activar (PowerShell)
nombre_entorno_virtual\Scripts\Activate.ps1
```

> **Posible error en PowerShell** — si aparece un error sobre ejecución de scripts deshabilitada, ejecutar primero:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```
> Confirmar con `S` y volver a intentar la activación.

Cuando el entorno esté activo el prompt mostrará `(nombre_entorno_virtual)` al principio. Para desactivarlo:

```powershell
deactivate
```

### Instalar las librerías

Con el entorno virtual activo:

```powershell
pip install pandas sqlalchemy psycopg2-binary
```

| Librería | Para qué sirve |
|----------|----------------|
| `pandas` | Leer los CSVs, transformar los datos en DataFrames y aplicar toda la lógica ETL (filtros, merges, fillna, cálculos de columnas) |
| `sqlalchemy` | ORM y motor de conexión a PostgreSQL. Permite ejecutar SQL directamente desde Python y usar `to_sql()` para insertar DataFrames enteros en la base de datos |
| `psycopg2-binary` | Driver nativo de PostgreSQL para Python. SQLAlchemy lo usa internamente para comunicarse con la base de datos. La versión `-binary` incluye todas las dependencias compiladas, por lo que **no requiere instalar** nada adicional del sistema |

---

## 🐍 4. Instalar Python y dependencias en Linux (ubuntu)

```bash
sudo apt install python3 python3-pip python3-venv -y
```

### Crear y activar el entorno virtual

```bash
# Desde la raíz del proyecto
python3 -m venv .nombre_entorno_virtual

# Activar el entorno virtual (necesario en cada sesión de terminal)
source venv/bin/activate

# Para desactivarlo cuando termines
deactivate
```

### Instalar las librerías

```bash
pip install pandas sqlalchemy psycopg2-binary
```

| Librería | Para qué sirve |
|----------|----------------|
| `pandas` | Leer los CSVs, transformar los datos en DataFrames y aplicar toda la lógica ETL (filtros, merges, fillna, cálculos de columnas) |
| `sqlalchemy` | ORM y motor de conexión a PostgreSQL. Permite ejecutar SQL directamente desde Python y usar `to_sql()` para insertar DataFrames enteros en la base de datos |
| `psycopg2-binary` | Driver nativo de PostgreSQL para Python. SQLAlchemy lo usa internamente para comunicarse con la base de datos. La versión `-binary` incluye todas las dependencias compiladas, por lo que **no requiere instalar** `libpq-dev` ni compiladores del sistema |

---

## 🏗️ 5. Crear el esquema de la base de datos (DDL)

Antes de ejecutar el ETL, el esquema `dwh` y todas las tablas deben existir en PostgreSQL. Ejecutar el DDL proporcionado:

```bash
# Con el usuario postgres
sudo -u postgres psql -d football_dwh -f ddl_dwh_schema.sql

# O si usas un usuario propio con el entorno virtual activo
psql -h localhost -U alberto -d football_dwh -f ddl_dwh_schema.sql
```

Esto crea el esquema `dwh` con todas las tablas, claves primarias, claves foráneas e índices necesarios.

---

## ▶️ 6. Orden de ejecución en Windows

Abrir **PowerShell** o **CMD**, navegar a la raíz del proyecto y activar el entorno virtual:

```powershell
nombre_entorno_virtual\Scripts\activate.bat
```

### Opción A — Ejecución automática completa (recomendado)

El script `run_etl_full.py` ejecuta todos los pasos en el orden correcto, trunca las tablas antes de cargar y para ante cualquier error:

```powershell
cd Proyecto\etl
python run_etl_full.py
```

El orquestador pedirá confirmación antes de comenzar y mostrará un resumen final con el estado de cada script.

---

### Opción B — Ejecución manual paso a paso

Si prefieres ejecutar cada script individualmente (útil para depurar o re-ejecutar solo una parte):

```powershell
cd Proyecto\etl
```

#### Paso 0 — Validar configuración

```powershell
python config.py
```

Comprueba que todos los CSVs existen y que la conexión a PostgreSQL funciona. Si falla aquí, revisar la ruta de los CSVs en `BASE_DIR` y la contraseña en `DB_CONFIG`.

#### Paso 1 — Generar dimensión fecha

```powershell
python generate_dim_date.py
```

Genera 11.323 registros de fechas desde el 01/01/2000 hasta el 31/12/2030.

#### Paso 2 — Cargar dimensiones (en este orden exacto)

Las dimensiones deben cargarse antes que los hechos porque las tablas de hechos tienen claves foráneas hacia ellas. El orden dentro de las dimensiones también importa (`dim_games` depende de `dim_clubs` y `dim_competitions`):

```powershell
python etl_dim_competitions.py   # 1º — no depende de ninguna otra dimensión
python etl_dim_clubs.py          # 2º — no depende de ninguna otra dimensión
python etl_dim_players.py        # 3º — no depende de ninguna otra dimensión
python etl_dim_games.py          # 4º — depende de dim_clubs y dim_competitions
```

> `etl_dim_clubs.py` y `etl_dim_players.py` insertan al final un **registro centinela** (`club_id = -1`, `player_id = -1`) que permite referenciar clubes o jugadores fuera del dataset sin violar las FK.

#### Paso 3 — Cargar tablas de hechos (en este orden exacto)

```powershell
python etl_fact_games.py             # 1º — depende de dim_games, dim_clubs, dim_competitions, dim_date
python etl_fact_appearances.py       # 2º — depende de dim_games, dim_players, dim_clubs
python etl_fact_game_events.py       # 3º — depende de dim_games, dim_players, dim_clubs
python etl_fact_transfers.py         # 4º — depende de dim_players, dim_clubs, dim_date
python etl_fact_player_valuations.py # 5º — depende de dim_players, dim_clubs, dim_competitions, dim_date
```

#### Paso 4 — Validación post-carga

```powershell
python validate_dwh.py       # Conteos, integridad referencial y estadísticas básicas
python check_nulls_final.py  # Auditoría de NULLs en todas las tablas del DWH
```

---

## ▶️ 6. Orden de ejecución en Linux (Ubuntu)

### Opción A — Ejecución automática completa (recomendado)

El script `run_etl_full.py` ejecuta todos los pasos en el orden correcto, trunca las tablas antes de cargar y para ante cualquier error:

```bash
# Con el entorno virtual activado
source venv/bin/activate
cd Proyecto/etl

python3 run_etl_full.py
```

El orquestador pedirá confirmación antes de comenzar y mostrará un resumen final con el estado de cada script.

---

### Opción B — Ejecución manual paso a paso

Si prefieres ejecutar cada script individualmente (útil para depurar o re-ejecutar solo una parte):

```bash
cd Proyecto/etl
source ../../venv/bin/activate
```

#### Paso 0 — Validar configuración

```bash
python3 config.py
```

Comprueba que todos los CSVs existen y que la conexión a PostgreSQL funciona. Si falla aquí, revisar la ruta de los CSVs en `BASE_DIR` y la contraseña en `DB_CONFIG`.

#### Paso 1 — Generar dimensión fecha

```bash
python3 generate_dim_date.py
```

Genera 11.323 registros de fechas desde el 01/01/2000 hasta el 31/12/2030.

#### Paso 2 — Cargar dimensiones (en este orden exacto)

Las dimensiones deben cargarse antes que los hechos porque las tablas de hechos tienen claves foráneas hacia ellas. El orden dentro de las dimensiones también importa (dim_games depende de dim_clubs y dim_competitions):

```bash
python3 etl_dim_competitions.py   # 1º — no depende de ninguna otra dimensión
python3 etl_dim_clubs.py          # 2º — no depende de ninguna otra dimensión
python3 etl_dim_players.py        # 3º — no depende de ninguna otra dimensión
python3 etl_dim_games.py          # 4º — depende de dim_clubs y dim_competitions
```

> `etl_dim_clubs.py` y `etl_dim_players.py` insertan al final un **registro centinela** (`club_id = -1`, `player_id = -1`) que permite referenciar clubes o jugadores fuera del dataset sin violar las FK.

#### Paso 3 — Cargar tablas de hechos (en este orden exacto)

```bash
python3 etl_fact_games.py             # 1º — depende de dim_games, dim_clubs, dim_competitions, dim_date
python3 etl_fact_appearances.py       # 2º — depende de dim_games, dim_players, dim_clubs
python3 etl_fact_game_events.py       # 3º — depende de dim_games, dim_players, dim_clubs
python3 etl_fact_transfers.py         # 4º — depende de dim_players, dim_clubs, dim_date
python3 etl_fact_player_valuations.py # 5º — depende de dim_players, dim_clubs, dim_competitions, dim_date
```

#### Paso 4 — Validación post-carga

```bash
python3 validate_dwh.py       # Conteos, integridad referencial y estadísticas básicas
python3 check_nulls_final.py  # Auditoría de NULLs en todas las tablas del DWH
```

---

## 🔄 7. Re-ejecutar el ETL (datos ya cargados)

El orquestador `run_etl_full.py` ejecuta automáticamente un `TRUNCATE CASCADE` en todas las tablas antes de empezar, en el orden correcto (primero hechos, luego dimensiones). No hace falta limpiar manualmente.

Si quieres limpiar la base de datos sin recargar, puedes ejecutar directamente en `psql`:

```sql
TRUNCATE TABLE dwh.fact_game_events, dwh.fact_appearances,
               dwh.fact_transfers, dwh.fact_player_valuations,
               dwh.fact_games, dwh.dim_games, dwh.dim_players,
               dwh.dim_clubs, dwh.dim_competitions, dwh.dim_date
CASCADE;
```

---

## 🔍 8. Scripts de análisis y diagnóstico

Estos scripts no forman parte del ETL principal pero son útiles para entender los datos y depurar:

| Script | Cuándo usarlo |
|--------|---------------|
| `check_nulls_final.py` | Auditar que no hay NULLs no deseados en ninguna tabla del DWH |
| `validate_dwh.py` | Verificar conteos, integridad referencial y estadísticas básicas post-carga |
| `informe_filtraciones.py` | Analizar en detalle por qué se filtraron registros en cada tabla durante el ETL |
| `comparacion_filtrados_validos.py` | Comparar partidos cargados vs filtrados con ejemplos concretos |
| `ejemplo_filtrado.py` | Ver el caso concreto de un partido filtrado y su impacto en cascada |

```bash
python3 check_nulls_final.py
python3 informe_filtraciones.py
```

---

## 📊 9. Consultas OLAP

El archivo `consultas_olap.sql` contiene **54 consultas** organizadas en 13 secciones:

| Sección | Operación | Nº consultas |
|---------|-----------|-------------|
| 1 | SLICE | 3 |
| 2 | DICE | 3 |
| 3 | ROLL-UP | 3 |
| 4 | DRILL-DOWN | 3 |
| 5 | PIVOT | 3 |
| 6 | WINDOW FUNCTIONS | 5 |
| 7 | GROUPING SETS / CUBE / ROLLUP | 3 |
| 8 | Análisis estratégicos combinados | 5 |
| 9 | Consultas estrella presentables | 10 |
| 10 | Apéndice técnico / auditoría Kimball | 1 |
| 11 | Análisis individual de jugadores | 5 |
| 12 | Equipos, competición y táctica | 5 |
| 13 | Mercado de fichajes avanzado | 5 |

Ejecutar desde `psql` o cualquier cliente SQL (DBeaver, DataGrip, pgAdmin):

```bash
psql -h localhost -U postgres -d football_dwh -f consultas_olap.sql | more
```

---

## ⚠️ Solución de problemas frecuentes en Windows

**Error: `password authentication failed`**
--> Revisar la contraseña en `config.py` --> `DB_CONFIG['password']`.

**Error: `could not connect to server`**
-->  Verificar que el servicio PostgreSQL está corriendo: Administrador de tareas -->  Servicios -->  `postgresql-x64-XX` debe estar **En ejecución**. Si no, iniciarlo con `Start-Service -Name "postgresql-x64-17"` en PowerShell como administrador.

**Error: `schema "dwh" does not exist`**
-->  El DDL no se ha ejecutado todavía. Ver paso 5.

**Error: `duplicate key value violates unique constraint`**
-->  Las tablas tienen datos de una ejecución anterior. El orquestador hace TRUNCATE automáticamente, pero si ejecutas scripts individuales debes limpiar primero o usar `run_etl_full.py`.

**Error: `FileNotFoundError: players.csv not found`**
-->  La estructura de directorios no coincide con la esperada. Verificar que los CSVs están en `Proyecto\Formato_csv\` y que los scripts se ejecutan desde `Proyecto\etl\`.

**Error: `'python' is not recognized as an internal or external command`**
-->  Python no está en el PATH. Reinstalar Python marcando **"Add Python to PATH"** durante la instalación, o añadirlo manualmente a las variables de entorno del sistema.

**Error al activar el entorno virtual en PowerShell (`running scripts is disabled`)**
-->  Ejecutar `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser` y confirmar con `S`.

**El entorno virtual no está activo**
-->  Ejecutar `nombre_entorno_virtual\Scripts\activate.bat` (CMD) o `nombre_entorno_virtual\Scripts\Activate.ps1` (PowerShell) desde la raíz del proyecto antes de cualquier script Python. El prompt mostrará `(nombre_entorno_virtual)` cuando esté activo.

## ⚠️ Solución de problemas frecuentes en Linux (Ubuntu)

**Error: `password authentication failed`**
--> Revisar la contraseña en `config.py` --> `DB_CONFIG['password']`.

**Error: `could not connect to server`**
--> Verificar que PostgreSQL está corriendo: `sudo systemctl status postgresql`

**Error: `schema "dwh" does not exist`**
--> El DDL no se ha ejecutado todavía. Ver paso 5.

**Error: `duplicate key value violates unique constraint`**
--> Las tablas tienen datos de una ejecución anterior. El orquestador hace TRUNCATE automáticamente, pero si ejecutas scripts individuales debes limpiar primero o usar `run_etl_full.py`.

**Error: `FileNotFoundError: players.csv not found`**
--> La estructura de directorios no coincide con la esperada. Verificar que los CSVs están en `Proyecto/Formato_csv/` y que `config.py` se ejecuta desde `Proyecto/etl/`.

**El entorno virtual no está activo**
--> Ejecutar `source venv/bin/activate` desde la raíz del proyecto antes de cualquier script Python. El prompt de la terminal mostrará `(venv)` cuando esté activo.
