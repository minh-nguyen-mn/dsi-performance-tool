import pandas as pd
import json
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class FoodSecurity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, index=True)
    inc = db.Column(db.String(100))
    states = db.Column(db.String(2), nullable=False, index=True)
    edu = db.Column(db.String(100))
    sexes = db.Column(db.String(100))
    races = db.Column(db.String(100))
    jobs = db.Column(db.String(100))
    cit = db.Column(db.String(100))
    dis = db.Column(db.String(100))
    ind = db.Column(db.String(100))
    food = db.Column(db.String(100))
    security = db.Column(db.String(100))

    def __repr__(self):
        return f'<FoodSecurity {self.year}, {self.states}>'


class DataProcessor:
    def __init__(self, filepaths):
        self.filepaths = filepaths
        self.load_variable_mappings()

    def load_variable_mappings(self):
        with open("foodsecurityconfig.json", "r") as f:
            mappings = json.load(f)
        self.variable_mappings = {
            "columns": mappings["columns"],
            "rename_columns":  mappings["rename_columns"],
            # Convert string keys to integers
            "mappings": {column: {int(key): value for key, value in mapping.items()} for column, mapping in mappings["mappings"].items()}
        }

    def process_csv(self):
        # Concatenate the dataframes
        df = pd.DataFrame(columns=self.variable_mappings["columns"])
        for filepath in self.filepaths:
            data = pd.read_csv(filepath)
            data.rename(columns=self.variable_mappings["rename_columns"], inplace=True)
            data = data[self.variable_mappings["columns"]]
            df = pd.concat([df, data])

        # Apply the transformations using the mapping dictionaries
        for column, mapping in self.variable_mappings["mappings"].items():
            df[column] = df[column].map(mapping)
            
        # Create a list of dictionaries representing the rows to be inserted
        rows_to_insert = []
        for _, row in df.iterrows():
            rows_to_insert.append({
                "year": row['year'],
                "inc": row['inc'],
                "states": row['states'],
                "edu": row['edu'],
                "sexes": row['sexes'],
                "races": row['races'],
                "jobs": row['jobs'],
                "cit": row['cit'],
                "dis": row['dis'],
                "ind": row['ind'],
                "food": row['food'],
                "security": row['security']
            })

        chunk_size = 1000  # Adjust this based on system's performance
        for i in range(0, len(rows_to_insert), chunk_size):
            chunk = rows_to_insert[i:i + chunk_size]
            db.session.bulk_insert_mappings(FoodSecurity, chunk) # Use bulk_insert_mappings for efficient insertion

        db.session.commit()  # Commit the changes to the database
