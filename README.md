# ETL FINAL PROJECT
Maestría en Inteligencia Artificial y Ciencia de Datos, clase ETL - Final Class Project

## Follow these steps before running your notebook (Mac OS X - M1)


#### 1. Creating a Python Environment:
```bash
$ python3 -m venv venv
```

#### 2. Activate the Python environment:
```bash
$ source venv/bin/activate
```

#### 3. Install dependencies from requirements.txt file:
```bash
$ pip install -r requirements.txt
```

#### 4. Make a copy of the .env.example file to .env and add the necessary settings:
```bash
$ cp .env.example .env
```

#### 5. Raise the local PostgreSQL database:
```bash
$ docker compose up -d
```

## Optional

#### Shutdown/stop local PostgreSQL database:
```bash
$ docker compose down
```

#### Disable Python environment:
```bash
$ deactivate
```

#### Run the script that generate a new Mascotas Propietarios CSV file:
```bash
$ cd src/main/python/generate_datasources
$ python3 script_1_generate_mascotas_propietarios.py
```

#### Run the script that generate a new Propietarios Transacciones CSV file:
```bash
$ cd src/main/python/generate_datasources
$ python3 script_2_generate_propietarios_transacciones.py
```