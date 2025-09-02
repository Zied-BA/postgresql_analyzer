"""
Regression Generator Module
Generates synthetic test data for regression testing and data quality validation.
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector
from db.schema_reader import SchemaReader


class RegressionGenerator:
    """Generates synthetic test data for regression testing and validation."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize regression generator with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.schema_reader = SchemaReader(db_connector)
        self.logger = logging.getLogger(__name__)
        
        # Sample data for synthetic generation
        self.sample_data = {
            'first_names': [
                "John", "Jane", "Mike", "Sarah", "David", "Lisa", "Tom", "Amy",
                "Chris", "Maria", "James", "Jennifer", "Robert", "Linda", "William",
                "Elizabeth", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Karen"
            ],
            'last_names': [
                "Smith", "Doe", "Johnson", "Wilson", "Brown", "Davis", "Miller",
                "Garcia", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
                "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Clark"
            ],
            'email_domains': [
                "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"
            ],
            'film_titles': [
                "The Matrix", "Inception", "The Shawshank Redemption", "Pulp Fiction",
                "Fight Club", "Forrest Gump", "The Godfather", "Goodfellas",
                "The Silence of the Lambs", "Schindler's List", "The Green Mile",
                "The Usual Suspects", "Se7en", "The Sixth Sense", "American Beauty",
                "Gladiator", "The Lord of the Rings", "Titanic", "Avatar", "Interstellar"
            ],
            'film_categories': [
                "Action", "Drama", "Comedy", "Thriller", "Horror", "Romance",
                "Sci-Fi", "Documentary", "Animation", "Adventure", "Crime", "Mystery"
            ],
            'street_names': [
                "Main St", "Oak Ave", "Elm St", "Pine Rd", "Cedar Ln", "Maple Dr",
                "Washington Blvd", "Park Ave", "Broadway", "5th Ave", "Lexington Ave"
            ],
            'cities': [
                "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
                "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville"
            ],
            'states': [
                "NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "GA", "NC", "MI", "NJ"
            ]
        }
    
    def generate_customer_data(self, count: int = 100, include_issues: bool = False) -> List[Dict[str, Any]]:
        """
        Generate synthetic customer data with optional data quality issues.
        
        Args:
            count: Number of customers to generate
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            List of customer dictionaries
        """
        customers = []
        
        for i in range(count):
            customer_id = i + 1
            
            # Generate basic customer data
            first_name = random.choice(self.sample_data['first_names'])
            last_name = random.choice(self.sample_data['last_names'])
            
            # Generate email (with potential issues)
            if include_issues and random.random() < 0.1:  # 10% missing emails
                email = None
            else:
                email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(self.sample_data['email_domains'])}"
            
            # Generate address data
            address_id = random.randint(1, 100)
            
            # Generate create date with some variation
            create_date = datetime.now() - timedelta(days=random.randint(1, 365))
            
            customer = {
                'customer_id': customer_id,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'address_id': address_id,
                'active': random.choice([True, False]),
                'create_date': create_date,
                'last_update': datetime.now()
            }
            
            # Add data quality issues if requested
            if include_issues:
                if random.random() < 0.05:  # 5% missing first names
                    customer['first_name'] = None
                if random.random() < 0.03:  # 3% missing last names
                    customer['last_name'] = None
                if random.random() < 0.02:  # 2% duplicate emails
                    customer['email'] = "duplicate@example.com"
            
            customers.append(customer)
        
        return customers
    
    def generate_film_data(self, count: int = 50, include_issues: bool = False) -> List[Dict[str, Any]]:
        """
        Generate synthetic film data with optional data quality issues.
        
        Args:
            count: Number of films to generate
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            List of film dictionaries
        """
        films = []
        
        for i in range(count):
            film_id = i + 1
            
            # Generate basic film data
            title = random.choice(self.sample_data['film_titles'])
            description = f"A compelling story about {random.choice(['love', 'adventure', 'mystery', 'friendship'])}"
            release_year = random.randint(1990, 2023)
            
            film = {
                'film_id': film_id,
                'title': title,
                'description': description,
                'release_year': release_year,
                'language_id': random.randint(1, 5),
                'rental_duration': random.randint(1, 7),
                'rental_rate': round(random.uniform(0.99, 4.99), 2),
                'length': random.randint(60, 180),
                'replacement_cost': round(random.uniform(9.99, 29.99), 2),
                'rating': random.choice(['G', 'PG', 'PG-13', 'R', 'NC-17']),
                'last_update': datetime.now()
            }
            
            # Add data quality issues if requested
            if include_issues:
                if random.random() < 0.08:  # 8% missing titles
                    film['title'] = None
                if random.random() < 0.05:  # 5% missing release years
                    film['release_year'] = None
                if random.random() < 0.03:  # 3% duplicate titles
                    film['title'] = "Duplicate Film Title"
            
            films.append(film)
        
        return films
    
    def generate_rental_data(self, count: int = 200, include_issues: bool = False) -> List[Dict[str, Any]]:
        """
        Generate synthetic rental data with optional data quality issues.
        
        Args:
            count: Number of rentals to generate
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            List of rental dictionaries
        """
        rentals = []
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(count):
            rental_id = i + 1
            
            # Generate rental date
            rental_date = base_date + timedelta(days=random.randint(0, 30))
            
            # Generate return date (with potential issues)
            if include_issues and random.random() < 0.15:  # 15% missing returns
                return_date = None
            else:
                return_date = rental_date + timedelta(days=random.randint(1, 14))
            
            rental = {
                'rental_id': rental_id,
                'rental_date': rental_date,
                'inventory_id': random.randint(1, 100),
                'customer_id': random.randint(1, 100),
                'return_date': return_date,
                'staff_id': random.randint(1, 5),
                'last_update': datetime.now()
            }
            
            # Add data quality issues if requested
            if include_issues:
                if random.random() < 0.02:  # 2% missing rental dates
                    rental['rental_date'] = None
                if random.random() < 0.01:  # 1% future rental dates
                    rental['rental_date'] = datetime.now() + timedelta(days=random.randint(1, 30))
            
            rentals.append(rental)
        
        return rentals
    
    def generate_payment_data(self, count: int = 150, include_issues: bool = False) -> List[Dict[str, Any]]:
        """
        Generate synthetic payment data with optional data quality issues.
        
        Args:
            count: Number of payments to generate
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            List of payment dictionaries
        """
        payments = []
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(count):
            payment_id = i + 1
            
            # Generate payment data
            customer_id = random.randint(1, 100)
            rental_id = random.randint(1, 200)
            amount = round(random.uniform(0.99, 9.99), 2)
            payment_date = base_date + timedelta(days=random.randint(0, 30))
            
            payment = {
                'payment_id': payment_id,
                'customer_id': customer_id,
                'rental_id': rental_id,
                'amount': amount,
                'payment_date': payment_date,
                'staff_id': random.randint(1, 5)
            }
            
            # Add data quality issues if requested
            if include_issues:
                if random.random() < 0.05:  # 5% missing amounts
                    payment['amount'] = None
                if random.random() < 0.03:  # 3% negative amounts
                    payment['amount'] = -abs(amount)
                if random.random() < 0.02:  # 2% missing payment dates
                    payment['payment_date'] = None
            
            payments.append(payment)
        
        return payments
    
    def generate_address_data(self, count: int = 50, include_issues: bool = False) -> List[Dict[str, Any]]:
        """
        Generate synthetic address data with optional data quality issues.
        
        Args:
            count: Number of addresses to generate
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            List of address dictionaries
        """
        addresses = []
        
        for i in range(count):
            address_id = i + 1
            
            # Generate address data
            street_number = random.randint(1, 9999)
            street_name = random.choice(self.sample_data['street_names'])
            city = random.choice(self.sample_data['cities'])
            state = random.choice(self.sample_data['states'])
            postal_code = f"{random.randint(10000, 99999)}"
            
            address = {
                'address_id': address_id,
                'address': f"{street_number} {street_name}",
                'address2': None,
                'district': f"District {random.randint(1, 10)}",
                'city_id': random.randint(1, 20),
                'postal_code': postal_code,
                'phone': f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                'last_update': datetime.now()
            }
            
            # Add data quality issues if requested
            if include_issues:
                if random.random() < 0.07:  # 7% missing addresses
                    address['address'] = None
                if random.random() < 0.04:  # 4% missing postal codes
                    address['postal_code'] = None
                if random.random() < 0.03:  # 3% missing phones
                    address['phone'] = None
            
            addresses.append(address)
        
        return addresses
    
    def create_regression_dataset(self, include_issues: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """
        Create a complete regression dataset with all table data.
        
        Args:
            include_issues: Whether to include data quality issues for testing
            
        Returns:
            Dictionary containing all generated data
        """
        self.logger.info("Generating regression dataset...")
        
        dataset = {
            'addresses': self.generate_address_data(50, include_issues),
            'customers': self.generate_customer_data(100, include_issues),
            'films': self.generate_film_data(50, include_issues),
            'rentals': self.generate_rental_data(200, include_issues),
            'payments': self.generate_payment_data(150, include_issues)
        }
        
        self.logger.info(f"Generated regression dataset with {sum(len(data) for data in dataset.values())} total records")
        return dataset
    
    def insert_regression_data(self, dataset: Dict[str, List[Dict[str, Any]]], 
                             schema: str = 'public') -> Dict[str, int]:
        """
        Insert regression dataset into database tables.
        
        Args:
            dataset: Dictionary containing data for each table
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary with insertion counts for each table
        """
        results = {}
        
        for table_name, data in dataset.items():
            if not data:
                continue
            
            try:
                # Get column names from the first data item
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                query = f"INSERT INTO {schema}.{table_name} ({column_names}) VALUES ({placeholders})"
                
                # Prepare data for insertion
                values = []
                for row in data:
                    row_values = []
                    for column in columns:
                        value = row[column]
                        row_values.append(value)
                    values.append(tuple(row_values))
                
                with self.db.get_cursor() as cursor:
                    cursor.executemany(query, values)
                    inserted_count = cursor.rowcount
                    results[table_name] = inserted_count
                    self.logger.info(f"Inserted {inserted_count} rows into {schema}.{table_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to insert data into {schema}.{table_name}: {e}")
                results[table_name] = 0
        
        return results
    
    def generate_data_quality_scenarios(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate specific data quality scenarios for testing.
        
        Returns:
            Dictionary containing data quality test scenarios
        """
        scenarios = {
            'missing_data': {
                'customers': [
                    {'customer_id': 9991, 'first_name': None, 'last_name': 'Missing', 'email': 'test@example.com'},
                    {'customer_id': 9992, 'first_name': 'Missing', 'last_name': None, 'email': 'test2@example.com'},
                    {'customer_id': 9993, 'first_name': 'Missing', 'last_name': 'Email', 'email': None}
                ],
                'rentals': [
                    {'rental_id': 9991, 'rental_date': None, 'customer_id': 1, 'inventory_id': 1},
                    {'rental_id': 9992, 'rental_date': datetime.now(), 'customer_id': 1, 'inventory_id': 1, 'return_date': None}
                ]
            },
            'duplicate_data': {
                'customers': [
                    {'customer_id': 9994, 'first_name': 'Duplicate', 'last_name': 'User', 'email': 'duplicate@example.com'},
                    {'customer_id': 9995, 'first_name': 'Duplicate', 'last_name': 'User', 'email': 'duplicate@example.com'}
                ],
                'films': [
                    {'film_id': 9991, 'title': 'Duplicate Film', 'release_year': 2020},
                    {'film_id': 9992, 'title': 'Duplicate Film', 'release_year': 2020}
                ]
            },
            'invalid_data': {
                'payments': [
                    {'payment_id': 9991, 'customer_id': 1, 'rental_id': 1, 'amount': -5.00},
                    {'payment_id': 9992, 'customer_id': 1, 'rental_id': 1, 'amount': 0.00}
                ],
                'rentals': [
                    {'rental_id': 9993, 'rental_date': datetime.now() + timedelta(days=30), 'customer_id': 1, 'inventory_id': 1}
                ]
            }
        }
        
        return scenarios
    
    def cleanup_regression_data(self, table_names: List[str] = None, schema: str = 'public') -> Dict[str, int]:
        """
        Clean up regression test data from specified tables.
        
        Args:
            table_names: List of table names to clean (default: all test tables)
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary with deletion counts for each table
        """
        if table_names is None:
            table_names = ['customer', 'film', 'rental', 'payment', 'address']
        
        results = {}
        
        for table_name in table_names:
            try:
                # Delete test data (records with ID > 9000)
                query = f"DELETE FROM {schema}.{table_name} WHERE {table_name}_id > 9000"
                deleted_count = self.db.execute_command(query)
                results[table_name] = deleted_count
                self.logger.info(f"Deleted {deleted_count} test rows from {schema}.{table_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to cleanup {schema}.{table_name}: {e}")
                results[table_name] = 0
        
        return results
