"""
Data Inserter Module
Handles insertion of synthetic test data for analysis and testing purposes.
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from .connector import DatabaseConnector


class DataInserter:
    """Handles insertion of synthetic data for testing and analysis."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize data inserter with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.logger = logging.getLogger(__name__)
        
        # Sample data for synthetic generation
        self.sample_names = [
            "John Smith", "Jane Doe", "Mike Johnson", "Sarah Wilson", "David Brown",
            "Lisa Davis", "Tom Miller", "Amy Garcia", "Chris Rodriguez", "Maria Martinez",
            "James Anderson", "Jennifer Taylor", "Robert Thomas", "Linda Jackson",
            "William White", "Elizabeth Harris", "Richard Martin", "Susan Thompson"
        ]
        
        self.sample_emails = [
            "john.smith@email.com", "jane.doe@email.com", "mike.johnson@email.com",
            "sarah.wilson@email.com", "david.brown@email.com", "lisa.davis@email.com",
            "tom.miller@email.com", "amy.garcia@email.com", "chris.rodriguez@email.com",
            "maria.martinez@email.com", "james.anderson@email.com", "jennifer.taylor@email.com"
        ]
        
        self.sample_titles = [
            "The Matrix", "Inception", "The Shawshank Redemption", "Pulp Fiction",
            "Fight Club", "Forrest Gump", "The Godfather", "Goodfellas",
            "The Silence of the Lambs", "Schindler's List", "The Green Mile",
            "The Usual Suspects", "Se7en", "The Sixth Sense", "American Beauty",
            "Gladiator", "The Lord of the Rings", "Titanic", "Avatar", "Interstellar"
        ]
        
        self.sample_categories = [
            "Action", "Drama", "Comedy", "Thriller", "Horror", "Romance",
            "Sci-Fi", "Documentary", "Animation", "Adventure", "Crime", "Mystery"
        ]
    
    def generate_synthetic_customers(self, count: int = 100) -> List[Dict[str, Any]]:
        """
        Generate synthetic customer data.
        
        Args:
            count: Number of customers to generate
            
        Returns:
            List of customer dictionaries
        """
        customers = []
        for i in range(count):
            customer = {
                'customer_id': i + 1,
                'first_name': random.choice(self.sample_names).split()[0],
                'last_name': random.choice(self.sample_names).split()[-1],
                'email': random.choice(self.sample_emails),
                'address_id': random.randint(1, 100),
                'active': random.choice([True, False]),
                'create_date': datetime.now() - timedelta(days=random.randint(1, 365)),
                'last_update': datetime.now()
            }
            customers.append(customer)
        return customers
    
    def generate_synthetic_films(self, count: int = 50) -> List[Dict[str, Any]]:
        """
        Generate synthetic film data.
        
        Args:
            count: Number of films to generate
            
        Returns:
            List of film dictionaries
        """
        films = []
        for i in range(count):
            film = {
                'film_id': i + 1,
                'title': random.choice(self.sample_titles),
                'description': f"Description for {random.choice(self.sample_titles)}",
                'release_year': random.randint(1990, 2023),
                'language_id': random.randint(1, 5),
                'rental_duration': random.randint(1, 7),
                'rental_rate': round(random.uniform(0.99, 4.99), 2),
                'length': random.randint(60, 180),
                'replacement_cost': round(random.uniform(9.99, 29.99), 2),
                'rating': random.choice(['G', 'PG', 'PG-13', 'R', 'NC-17']),
                'last_update': datetime.now()
            }
            films.append(film)
        return films
    
    def generate_synthetic_rentals(self, count: int = 200) -> List[Dict[str, Any]]:
        """
        Generate synthetic rental data with some missing returns.
        
        Args:
            count: Number of rentals to generate
            
        Returns:
            List of rental dictionaries
        """
        rentals = []
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(count):
            rental_date = base_date + timedelta(days=random.randint(0, 30))
            
            # 10% of rentals have missing returns (return_date is NULL)
            has_return = random.random() > 0.1
            
            rental = {
                'rental_id': i + 1,
                'rental_date': rental_date,
                'inventory_id': random.randint(1, 100),
                'customer_id': random.randint(1, 100),
                'return_date': rental_date + timedelta(days=random.randint(1, 14)) if has_return else None,
                'staff_id': random.randint(1, 5),
                'last_update': datetime.now()
            }
            rentals.append(rental)
        return rentals
    
    def insert_synthetic_data(self, table_name: str, data: List[Dict[str, Any]], schema: str = 'public') -> int:
        """
        Insert synthetic data into a table.
        
        Args:
            table_name: Name of the target table
            data: List of dictionaries containing data to insert
            schema: Schema name (default: 'public')
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0
        
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
                # Handle None values for nullable columns
                if value is None:
                    row_values.append(None)
                else:
                    row_values.append(value)
            values.append(tuple(row_values))
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.executemany(query, values)
                inserted_count = cursor.rowcount
                self.logger.info(f"Inserted {inserted_count} rows into {schema}.{table_name}")
                return inserted_count
        except Exception as e:
            self.logger.error(f"Failed to insert data into {schema}.{table_name}: {e}")
            raise
    
    def create_test_dataset(self, customer_count: int = 100, film_count: int = 50, rental_count: int = 200) -> Dict[str, int]:
        """
        Create a complete test dataset with customers, films, and rentals.
        
        Args:
            customer_count: Number of customers to generate
            film_count: Number of films to generate
            rental_count: Number of rentals to generate
            
        Returns:
            Dictionary with insertion counts for each table
        """
        results = {}
        
        try:
            # Generate and insert customers
            self.logger.info("Generating synthetic customers...")
            customers = self.generate_synthetic_customers(customer_count)
            results['customers'] = self.insert_synthetic_data('customer', customers)
            
            # Generate and insert films
            self.logger.info("Generating synthetic films...")
            films = self.generate_synthetic_films(film_count)
            results['films'] = self.insert_synthetic_data('film', films)
            
            # Generate and insert rentals
            self.logger.info("Generating synthetic rentals...")
            rentals = self.generate_synthetic_rentals(rental_count)
            results['rentals'] = self.insert_synthetic_data('rental', rentals)
            
            self.logger.info(f"Test dataset created successfully: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to create test dataset: {e}")
            raise
    
    def insert_missing_data_scenarios(self) -> Dict[str, int]:
        """
        Insert specific data scenarios to test missing data detection.
        
        Returns:
            Dictionary with insertion counts
        """
        results = {}
        
        # Insert customers with missing email addresses
        customers_missing_email = [
            {
                'customer_id': 9991,
                'first_name': 'Missing',
                'last_name': 'Email',
                'email': None,
                'address_id': 1,
                'active': True,
                'create_date': datetime.now(),
                'last_update': datetime.now()
            },
            {
                'customer_id': 9992,
                'first_name': 'Another',
                'last_name': 'Missing',
                'email': None,
                'address_id': 2,
                'active': True,
                'create_date': datetime.now(),
                'last_update': datetime.now()
            }
        ]
        
        try:
            results['customers_missing_email'] = self.insert_synthetic_data('customer', customers_missing_email)
            
            # Insert rentals with missing return dates (overdue rentals)
            overdue_rentals = [
                {
                    'rental_id': 9991,
                    'rental_date': datetime.now() - timedelta(days=30),
                    'inventory_id': 1,
                    'customer_id': 1,
                    'return_date': None,
                    'staff_id': 1,
                    'last_update': datetime.now()
                },
                {
                    'rental_id': 9992,
                    'rental_date': datetime.now() - timedelta(days=45),
                    'inventory_id': 2,
                    'customer_id': 2,
                    'return_date': None,
                    'staff_id': 1,
                    'last_update': datetime.now()
                }
            ]
            
            results['overdue_rentals'] = self.insert_synthetic_data('rental', overdue_rentals)
            
            self.logger.info(f"Missing data scenarios inserted: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to insert missing data scenarios: {e}")
            raise
    
    def cleanup_test_data(self, table_names: List[str] = None, schema: str = 'public') -> Dict[str, int]:
        """
        Clean up test data from specified tables.
        
        Args:
            table_names: List of table names to clean (default: all test tables)
            schema: Schema name (default: 'public')
            
        Returns:
            Dictionary with deletion counts for each table
        """
        if table_names is None:
            table_names = ['customer', 'film', 'rental']
        
        results = {}
        
        for table_name in table_names:
            try:
                # Delete test data (customers with ID > 9000, rentals with ID > 9000, etc.)
                if table_name == 'customer':
                    query = f"DELETE FROM {schema}.{table_name} WHERE customer_id > 9000"
                elif table_name == 'rental':
                    query = f"DELETE FROM {schema}.{table_name} WHERE rental_id > 9000"
                elif table_name == 'film':
                    query = f"DELETE FROM {schema}.{table_name} WHERE film_id > 9000"
                else:
                    query = f"DELETE FROM {schema}.{table_name} WHERE 1=1"
                
                deleted_count = self.db.execute_command(query)
                results[table_name] = deleted_count
                self.logger.info(f"Deleted {deleted_count} rows from {schema}.{table_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to cleanup {schema}.{table_name}: {e}")
                results[table_name] = 0
        
        return results
