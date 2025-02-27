import os
from dotenv import load_dotenv
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)  # Ensure reproducibility

# Define valid service types (excluding "Eutanasia")
SERVICES = [
    "Vacunación", "Venta Alimentos", "Cita Veterinario(a)", "Baño",
    "Desparacitada", "Peluqueada", "Otros"
]

# Special case: "Eutanasia" can only be used once per pet
EUTANASIA = "Eutanasia"

# Load generated data file
load_dotenv()
resources_path = os.getenv("RESOURCES_PATH")
print(f"Resources path: {resources_path}")

mascotas_propietarios_generated_file = f"{resources_path}/Mascotas_Propietarios_despensaAnimal_Generated.csv"
propietarios_ventas_generated_file = f"{resources_path}/Propietarios_Transacciones_despensaAnimal_Generated.csv"

try:
    # Read the CSV file
    df_mascotas = pd.read_csv(mascotas_propietarios_generated_file)

    # Ensure "Fecha de Nacimiento" is parsed correctly, fill missing with NaT
    df_mascotas["Fecha de Nacimiento"] = pd.to_datetime(df_mascotas["Fecha de Nacimiento"], errors="coerce")

except Exception as e:
    print(f"Error reading input file: {e}")
    exit(1)  # Exit script if file cannot be read

# Prepare a list for transaction data
transactions = []

# Current date for validation
today = datetime.today().date()

# Determine which 5% of pets will receive "Eutanasia"
total_pets = len(df_mascotas)
num_pets_with_euthanasia = int(total_pets * 0.08)  # 8% of total pets
pets_with_euthanasia = set(random.sample(range(total_pets), num_pets_with_euthanasia))

for index, row in df_mascotas.iterrows():
    try:
        # Fill missing values with Faker-generated data
        nombre_propietario = row["Nombre Propietario"] if pd.notna(row["Nombre Propietario"]) else fake.name()
        tipo_documento = row["Tipo de Documento"] if pd.notna(row["Tipo de Documento"]) else random.choice(["CC", "Pasaporte", "NIT", "CE"])
        numero_documento = row["Número de Documento"] if pd.notna(row["Número de Documento"]) else random.randint(100000000, 199999999)
        nombre_mascota = row["Nombre Mascota"] if pd.notna(row["Nombre Mascota"]) else fake.first_name()

        # Parse the birth date and handle missing values
        fecha_nacimiento = row["Fecha de Nacimiento"]

        if pd.isna(fecha_nacimiento):
            fecha_nacimiento = today - timedelta(days=random.randint(365, 365 * 15))  # Default: 1 to 15 years ago
        else:
            fecha_nacimiento = fecha_nacimiento.date()

        # Ensure birth date is not in the future
        if fecha_nacimiento >= today:
            fecha_nacimiento = today - timedelta(days=365)  # Default to at least 1 year old

        # Generate between 3 to 30 transactions for this client
        num_transactions = random.randint(3, 30)
        pet_transactions = []
        pet_has_euthanasia = index in pets_with_euthanasia  # Only 5% of pets

        for _ in range(num_transactions):
            # If the pet has already received "Eutanasia", stop generating more transactions for it
            if pet_has_euthanasia:
                break

            # Random service selection
            servicio_prestado = random.choice(SERVICES)

            # Random price between $5,000 and $2,000,000 COP
            valor_servicio = random.randint(5000, 2000000)

            # Generate a valid service date (between birth date and today)
            fecha_servicio = fake.date_between_dates(date_start=fecha_nacimiento, date_end=today)

            # Append transaction record
            pet_transactions.append({
                "Nombre Propietario": nombre_propietario,
                "Tipo de Documento": tipo_documento,
                "Número de Documento": numero_documento,
                "Nombre Mascota": nombre_mascota,
                "Servico Prestado": servicio_prestado,
                "Valor Servicio": valor_servicio,
                "Fecha del Servicio": fecha_servicio
            })

        # If the pet is among the 5% selected for "Eutanasia", ensure it is the last transaction
        if pet_has_euthanasia:
            max_date = max(t["Fecha del Servicio"] for t in pet_transactions) if pet_transactions else today
            euthanasia_transaction = {
                "Nombre Propietario": nombre_propietario,
                "Tipo de Documento": tipo_documento,
                "Número de Documento": numero_documento,
                "Nombre Mascota": nombre_mascota,
                "Servico Prestado": EUTANASIA,
                "Valor Servicio": random.randint(50000, 500000),  # Euthanasia costs between 50K and 500K
                "Fecha del Servicio": max_date
            }
            pet_transactions.append(euthanasia_transaction)

        # Add all transactions for this pet
        transactions.extend(pet_transactions)

    except Exception as e:
        print(f"Error processing row {row.to_dict()}: {e}")
        continue  # Skip row and continue processing

try:
    # Convert to DataFrame
    df_transactions = pd.DataFrame(transactions)

    # Save to CSV
    df_transactions.to_csv(propietarios_ventas_generated_file, index=False)

    print(f"Generated {len(df_transactions)} transactions and saved to {propietarios_ventas_generated_file}")

except Exception as e:
    print(f"Error saving transactions file: {e}")