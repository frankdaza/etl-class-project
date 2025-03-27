import os
from dotenv import load_dotenv
import pandas as pd
from faker import Faker
import random

def generate_colombian_cellphone():
    prefix = random.choice(["300", "301", "302", "303", "304", "305", "306", "307", "308", "309",
                            "310", "311", "312", "313", "314", "315", "316", "317", "318", "319",
                            "320", "321", "322", "323", "324"])
    number = f"{prefix}{random.randint(1000000, 9999999)}"  # Ensures 10-digit number
    return number

def generate_fake_row(fake: Faker) -> dict:
    """
    Generate a single row of fake data matching the required columns.
    Modify this function as needed to match the data format you want.
    """
    nombre_mascota = fake.first_name()  # Example: Using person's first name as a pet name
    raza = random.choice(["Golden Retriever", "Labrador", "Poodle", "German Shepherd", "Bulldog"])
    peso = round(random.uniform(1.0, 60.0), 2)  # Random float for weight
    fecha_nacimiento = fake.date_of_birth(minimum_age=0, maximum_age=20)  # Pet's birth date within 20 years
    sexo = random.choice(["Macho", "Hembra"])
    temperamento = random.choice(["Tranquilo", "Juguetón", "Agresivo", "Tímido", "Sociable"])
    numero_carnet = fake.uuid4()[:8].upper()  # A short random alphanumeric ID
    estado_reproductivo = random.choice(["Esterilizado", "No Esterilizado"])
    numero_partos = random.randint(0, 5) if sexo == "Hembra" else 0  # Example logic
    color = random.choice(["Blanco", "Negro", "Marrón", "Gris", "Beige"])
    
    # Fallecimiento fields (some might be None if still alive)
    # Example logic: 10% chance the pet has died
    fallecido = random.random() < 0.1
    fecha_fallecimiento = fake.date_between(start_date='-2y', end_date='today') if fallecido else ""
    motivo_fallecimiento = "Causas naturales" if fallecido else ""
    comentarios_fallecimiento = "Falleció durante la noche" if fallecido else ""

    nombre_propietario = fake.name()
    ciudad = "Cali" # There is only one Pet Store for La Despensa Animal in Cali.
    direccion = fake.street_address()
    telefono = generate_colombian_cellphone()
    email = fake.email()
    tipo_documento = random.choice(["CC", "Pasaporte", "NIT", "CE"])
    numero_documento = random.randint(100000000, 199999999)
    profesion = random.choice([
        "Ingeniero", 
        "Profesor", 
        "Médico", 
        "Diseñador", 
        "Abogado", 
        "Independiente", 
        "Peluquera", 
        "Abogada", 
        "Ingeniera Civil", 
        "Panadero", 
        "Músico", 
        "Arquitecta", 
        "Arquitecto",
        "Ama de Casa",
        "Estilista",
        "Estudiante"
        ])
    estado = random.choice(["Activo", "Inactivo"])
    
    # Notificaciones: let's say 50% chance set to True
    notificaciones_whatsapp = random.choice(["Si", "No"])

    return {
        "Nombre Mascota": nombre_mascota,
        "Raza": raza,
        "Peso": peso,
        "Fecha de Nacimiento": fecha_nacimiento,
        "Sexo": sexo,
        "Temperamento": temperamento,
        "Número de Carnet": numero_carnet,
        "Estado Reproductivo": estado_reproductivo,
        "Número de Partos": numero_partos,
        "Color": color,
        "Fecha de Fallecimiento": fecha_fallecimiento,
        "Motivo de Fallecimiento": motivo_fallecimiento,
        "Comentarios del Fallecimiento": comentarios_fallecimiento,
        "Nombre Propietario": nombre_propietario,
        "Ciudad": ciudad,
        "Dirección": direccion,
        "Télefono": telefono,
        "Email": email,
        "Tipo de Documento": tipo_documento,
        "Número de Documento": numero_documento,
        "Profesión": profesion,
        "Estado": estado,
        "Notificaciones Whatsapp": notificaciones_whatsapp
    }

def main():
    load_dotenv()
    resources_path = os.getenv("RESOURCES_PATH")
    print(f"Resources path: {resources_path}")

    mascotas_propietarios_original_file = f"{resources_path}/Mascotas_Propietarios_despensaanimal.xlsx"
    mascotas_propietarios_generated_file = f"{resources_path}/Mascotas_Propietarios_despensaAnimal_Generated.csv"

    # Initialize Faker
    fake = Faker()
    Faker.seed(0)

    # Read the original Excel file
    df_original = pd.read_excel(mascotas_propietarios_original_file)
    
    # We want at least 12,000 rows total
    desired_total_rows = 12000
    
    # Current number of rows in the original DataFrame
    current_rows = len(df_original)
    
    if current_rows >= desired_total_rows:
        print(f"Your original file already has {current_rows} rows, which is >= {desired_total_rows}!")        
        df_original.to_csv(mascotas_propietarios_generated_file, index=False)
        print(f"Data has been saved to {mascotas_propietarios_generated_file}")
        return
    
    # Calculate how many additional rows are needed
    rows_to_add = desired_total_rows - current_rows
    
    print(f"Original rows: {current_rows}")
    print(f"Adding {rows_to_add} new rows to reach {desired_total_rows}.")

    # List to store the new fake rows
    new_rows = []

    # Generate the additional rows
    for _ in range(rows_to_add):
        fake_row = generate_fake_row(fake)
        new_rows.append(fake_row)
    
    # Convert list of new rows to a DataFrame
    df_new = pd.DataFrame(new_rows)

    # Combine original data and new data
    df_expanded = pd.concat([df_original, df_new], ignore_index=True)

    # Save the combined DataFrame to a new CSV
    df_expanded.to_csv(mascotas_propietarios_generated_file, index=False)

    print(f"File '{mascotas_propietarios_generated_file}' created with {len(df_expanded)} rows.")

if __name__ == "__main__":
    main()