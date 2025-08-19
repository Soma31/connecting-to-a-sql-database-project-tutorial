import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar las variables de entorno
load_dotenv()

# 1) Conectar a la base de datos con SQLAlchemy
def connect():
    global engine
    try:
        connection_string = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("¡Conexión exitosa!")
        return engine
    except Exception as e:
        print(f"Error al conectar: {e}")
        return None

engine = connect()
if engine is None:
    exit()

# 2) Crear las tablas
with engine.connect() as connection:
    connection.execute("""
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS authors (
        author_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        middle_name VARCHAR(50) NULL,
        last_name VARCHAR(100) NULL
    );

    CREATE TABLE IF NOT EXISTS books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        total_pages INT NULL,
        rating DECIMAL(4, 2) NULL,
        isbn VARCHAR(13) NULL,
        published_date DATE,
        publisher_id INT NULL,
        CONSTRAINT fk_publisher FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS book_authors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY (book_id, author_id),
        CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
    );
    """)

# 3) Insertar datos de prueba
with engine.connect() as connection:
    connection.execute("""
    INSERT INTO publishers (name) VALUES ('Publisher A'), ('Publisher B') ON CONFLICT DO NOTHING;
    
    INSERT INTO authors (first_name, middle_name, last_name) VALUES 
    ('John', 'M.', 'Doe'), 
    ('Jane', NULL, 'Smith') ON CONFLICT DO NOTHING;
    
    INSERT INTO books (title, total_pages, rating, isbn, published_date, publisher_id) VALUES 
    ('Book One', 300, 4.5, '1234567890123', '2021-01-01', 1), 
    ('Book Two', 250, 4.0, '1234567890124', '2022-02-02', 2) ON CONFLICT DO NOTHING;
    
    INSERT INTO book_authors (book_id, author_id) VALUES 
    (1, 1), 
    (2, 2) ON CONFLICT DO NOTHING;
    """)

# 4) Convertir tablas a DataFrames de Pandas

df = pd.read_sql("SELECT * FROM publishers;", engine)
print(df)