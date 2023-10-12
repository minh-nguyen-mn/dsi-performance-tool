import os
import json
from flask import Flask, request
from data_processor import DataProcessor, db, FoodSecurity
from flask_restx import Api, Resource, fields, marshal
from sqlalchemy import func, and_, distinct

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'data.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'  # Show all tabs expanded

db.init_app(app)
api = Api(app, title='Food Security API', version='1.0', description='API for Food Security Data')

# Define the FoodSecurity model for Swagger documentation
food_security_model = api.model('FoodSecurity', {
    'year': fields.Integer(required=True, description='The year', index=True),
    'inc': fields.String(description='Income range'),
    'states': fields.String(required=True, description='State code', index=True),
    'edu': fields.String(description='Education level'),
    'sexes': fields.String(description='Gender'),
    'races': fields.String(description='Race'),
    'jobs': fields.String(description='Job status'),
    'cit': fields.String(description='Citizenship status'),
    'dis': fields.String(description='Disability status'),
    'ind': fields.String(description='Industry'),
    'food': fields.String(description='Food stamp receipt'),
    'security': fields.String(description='Food security level'),
}) 

# Load allowed values from foodsecurityconfig.json
with open("foodsecurityconfig.json", "r") as f:
    config_data = json.load(f)
allowed_states = list(config_data["mappings"]["states"].values())
allowed_years = list(config_data["years"])
full_to_abbrev = config_data["full_to_abbrev"]
allowed_factors = list(full_to_abbrev.keys())


@api.route("/filtered_data")
class FoodSecurityData(Resource):
    @api.doc(params={
        "state": {
            "required": True,
            "description": "State Code",
            "enum": ["All"] + allowed_states,
            "default": "All" 
        },
        "year": {
            "required": True,
            "description": "Year",
            "enum": ["All"] + allowed_years,
            "default": "All" 
        },
        "factor": {
            "required": True,
            "description": "Demographics and Socioeconomic Factor",
            "enum": ["None"] + allowed_factors,
            "default": "None"
        },
        "statistics": {
            "required": True,
            "description": "Type of Statistics",
            "enum": ["None", "Count", "Percentage"],
            "default": "None"
        },
        "limit": {
            "description": "Number of Records (Default is All)", 
            "type": int
        }
    })
    @api.doc(responses={200: "Success", 400: "Bad Request"})
    def get(self):
        # Get the parameters from the request
        state = request.args.get("state")
        year = request.args.get("year")
        factor_full_name = request.args.get("factor")    

        query = db.session.query(FoodSecurity)
        if state != "All" or year != "All":
            filters = []
            if state != "All":
                filters.append(FoodSecurity.states == state)
            if year != "All":
                filters.append(FoodSecurity.year == year)
            query = query.filter(and_(*filters))

        if factor_full_name == "None": 
            limit = int(request.args.get("limit", -1))
            if limit > 0:
                filtered_data = query.limit(limit).all()
            else:
                filtered_data = query.all()     
            # Convert the data to a JSON-serializable format using marshal
            serialized_data = [marshal(entry, food_security_model) for entry in filtered_data]
            return serialized_data, 200              
        else:
            statistics = request.args.get("statistics") 
            if statistics == "None":
                return {"error": "You must specify a value for 'statistical_type' when 'factor' is selected."}, 400
            
            factor = full_to_abbrev.get(factor_full_name) # Map the full name to the abbreviation
            subquery = query.filter(FoodSecurity.security.isnot(None))
            food_security_counts = subquery.with_entities(
                getattr(FoodSecurity, factor),
                FoodSecurity.security,
                func.count(FoodSecurity.id)
            ).group_by(getattr(FoodSecurity, factor), FoodSecurity.security).all()
    
            result = {}
            if statistics == "Count":
                for factor_value, security, count in food_security_counts:
                    result.setdefault(str(factor_value), {})[security] = count
            elif statistics == "Percentage":
                for factor_value, security, count in food_security_counts:
                    total_count = sum([item[2] for item in food_security_counts if item[0] == factor_value])
                    percentage = (count / total_count) * 100 if total_count > 0 else 0
                    result.setdefault(str(factor_value), {})[security] = "{:.2f}%".format(percentage)

            return result, 200


@api.route("/filtered_data_counts")
class FoodSecurityDataCounts(Resource):
    @api.doc(params={
        "state": {
            "required": True,
            "description": "State Code",
            "enum": ["All"] + allowed_states,
            "default": "All" 
        },
        "year": {
            "required": True,
            "description": "Year",
            "enum": ["All"] + allowed_years,
            "default": "All" 
        },
        "factor": {
            "required": True,
            "description": "Demographics & Socioeconomic Factor",
            "enum": ["None"] + allowed_factors,
            "default": "None"
        },
    })
    @api.response(200, 'Success')
    def get(self):
        # Get the parameters from the request
        state = request.args.get("state")
        year = request.args.get("year")
        factor_full_name = request.args.get("factor") 

        # Query the database to get filtered data
        query = FoodSecurity.query
        if state != "All":
            query = query.filter_by(states=state)
        if year != "All":
            query = query.filter_by(year=year)
            
        factor = full_to_abbrev.get(factor_full_name) # Map the full name to the abbreviation
        subquery = query.filter(FoodSecurity.security.isnot(None))
        food_security_counts = subquery.with_entities(
            getattr(FoodSecurity, factor),
            FoodSecurity.security,
            func.count(FoodSecurity.id)
        ).group_by(getattr(FoodSecurity, factor), FoodSecurity.security).all()
    
        result = {}
        for factor_value, security, count in food_security_counts:
            result.setdefault(str(factor_value), {})[security] = count
        return result, 200


@api.route("/filtered_data_percentages")
class FoodSecurityDataPercentages(Resource):
    @api.doc(params={
        "state": {
            "required": True,
            "description": "State Code",
            "enum": ["All"] + allowed_states,
            "default": "All" 
        },
        "year": {
            "required": True,
            "description": "Year",
            "enum": ["All"] + allowed_years,
            "default": "All" 
        },
        "factor": {
            "required": True,
            "description": "Demographics & Socioeconomic Factor",
            "enum": ["None"] + allowed_factors,
            "default": "None"
        },
    })
    @api.response(200, 'Success') 
    def get(self):
        # Get the parameters from the request
        state = request.args.get("state")
        year = request.args.get("year")
        factor_full_name = request.args.get("factor") 

        # Query the database to get filtered data
        query = FoodSecurity.query
        if state != "All":
            query = query.filter_by(states=state)
        if year != "All":
            query = query.filter_by(year=year)

        factor = full_to_abbrev.get(factor_full_name) # Map the full name to the abbreviation
        subquery = query.filter(FoodSecurity.security.isnot(None))
        food_security_counts = subquery.with_entities(
            getattr(FoodSecurity, factor),
            FoodSecurity.security,
            func.count(FoodSecurity.id)
        ).group_by(getattr(FoodSecurity, factor), FoodSecurity.security).all()
    
        result = {}
        for factor_value, security, count in food_security_counts:
            total_count = sum([item[2] for item in food_security_counts if item[0] == factor_value])
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            result.setdefault(str(factor_value), {})[security] = "{:.2f}%".format(percentage)
        return result, 200


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        data_processor = DataProcessor(["../dec19pub.csv", "../dec20pub.csv", "../dec21pub.csv"])
        data_processor.process_csv()
    app.run()