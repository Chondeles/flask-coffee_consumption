# model.py

from dataclasses import dataclass

@dataclass
class User:
    user_id: int
    email: str
    password: str
    is_admin: int = 0  # Default to 0, indicating a regular user

@dataclass
class Country:
    country_id: int
    country_name: str

@dataclass
class Coffee:
    coffee_id: int
    coffee_type: str

@dataclass
class Consumption:
    consumption_id: int
    coffee_id: int
    country_id: int
    year: int
    consumption: float  # Assuming consumption is a numeric value



