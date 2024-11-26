import os
import sys
import pandas as pd
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, JSON, ForeignKey, Date, DateTime
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from sqlalchemy import func

# Define the database file location
DB_FILE = "parsed_data.db"
DB_URI = f"sqlite:///{DB_FILE}"

# Database Setup
Base = declarative_base()

# Meta Data Table
class MetaDataTable(Base):
    __tablename__ = 'meta_data'
    id = Column(Integer, primary_key=True)
    sheet_name = Column(String, nullable=False)
    parsed_columns = Column(JSON, nullable=False)
    row_count = Column(Integer, nullable=False)
    parsed_at = Column(DateTime, default=datetime.utcnow)

# Parsed Data Table
class ParsedDataTable(Base):
    __tablename__ = 'parsed_data'
    id = Column(Integer, primary_key=True)
    category = Column(String, nullable=False)
    item_name = Column(String, nullable=False)
    qty = Column(Float, nullable=False)
    date = Column(Date, nullable=False)  # Date extracted from the file name
    metadata_id = Column(Integer, ForeignKey('meta_data.id'), nullable=False)
    data_metadata = Column(String)

# Initialize the SQLite database and tables
engine = create_engine(DB_URI)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Function to write data to the database
def write_data_to_db(file_path):
    session = Session()

    # Extract the date from the file name
    file_name = os.path.basename(file_path)
    parsed_date = extract_date_from_filename(file_name)

    # Read the Excel file
    df = pd.read_excel(file_path)
    # Check required columns
    required_columns = ['Category', 'Item Name', 'Qty']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing required columns. Ensure {required_columns} exist in the sheet.")

    # Extract relevant columns
    parsed_df = df[required_columns]
    parsed_records = parsed_df.to_dict(orient='records')

    # Extract metadata
    sheet_name = "Sheet1"  # Update as needed
    row_count = len(parsed_df)
    metadata_entry = MetaDataTable(
        sheet_name=sheet_name,
        parsed_columns=required_columns,
        row_count=row_count,
        parsed_at=datetime.utcnow()
    )
    session.add(metadata_entry)
    session.commit()

    # Write parsed data
    metadata_id = metadata_entry.id
    parsed_data_entries = [
        ParsedDataTable(
            category=record['Category'],
            item_name=record['Item Name'],
            qty=record['Qty'],
            date=parsed_date,  # Use the extracted date
            metadata_id=metadata_id
        )
        for record in parsed_records
    ]
    session.bulk_save_objects(parsed_data_entries)
    session.commit()
    #debugging point print all the data in the database. 
    query = session.query(ParsedDataTable).all()  
    for row in query:
        print(f"Category: {row.category}, Item Name: {row.item_name}, Qty: {row.qty}, Date: {row.date}")          
    session.close()

    print(f"Successfully parsed {row_count} rows from {file_path}.")

# Function to read data from the database
def read_data_from_db():
    session = Session()
    #merge the same category and item name and sum the qty from the dates
    query = session.query(
        ParsedDataTable.category,
        ParsedDataTable.item_name,
        func.sum(ParsedDataTable.qty)
    ).group_by(
        ParsedDataTable.category,
        ParsedDataTable.item_name
    ).all()
    for row in query:

        
        print(row)
   
    session.close()

# Helper function to extract date from the file name
def extract_date_from_filename(file_name):
    import re
    match = re.search(r'(\d{4}_\d{2}_\d{2})', file_name)
    if match:
        return datetime.strptime(match.group(1), '%Y_%m_%d').date()
    else:
        raise ValueError(f"No valid date found in file name: {file_name}")

# Example Usage
file_path = "Restaurant_item_tax_report_2024_11_20_07_28_10.xlsx"

# Write data to SQLite
write_data_to_db(file_path)

# Read and print data from SQLite
read_data_from_db()
