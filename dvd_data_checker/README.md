# DVD Data Checker

A comprehensive PostgreSQL data analysis tool designed specifically for DVD rental business data quality checks, missing value detection, duplicate analysis, and automated customer communication.

## Features

### 🔍 Data Quality Analysis
- **Missing Value Detection**: Identifies and reports missing data across all tables
- **Duplicate Record Analysis**: Finds duplicate records and primary key violations
- **Date Gap Analysis**: Detects time gaps in date/time columns
- **Data Validation**: Validates data against business rules

### 📊 DVD Rental Business Intelligence
- **Overdue Rental Detection**: Finds customers with missing DVD returns
- **Fee Calculation**: Automatically calculates overdue fees
- **Customer Segmentation**: Identifies high-risk customers with multiple overdue rentals
- **Rental History Analysis**: Provides detailed customer rental patterns

### 📧 Automated Communication
- **Email Template System**: Pre-built templates for different scenarios
- **Overdue Reminders**: Automated email generation for overdue rentals
- **Data Quality Notifications**: Alerts customers about missing account information
- **CSV Export**: Export email data for bulk sending

### 🛠️ Technical Features
- **PostgreSQL Integration**: Native PostgreSQL support with connection pooling
- **Configurable Analysis**: Customizable thresholds and business rules
- **Comprehensive Reporting**: Detailed reports in multiple formats
- **Test Data Generation**: Synthetic data generation for testing and validation

## Installation

### Prerequisites
- Python 3.8 or higher
- PostgreSQL database with DVD rental schema
- pip package manager

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd dvd_data_checker
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure database connection**
   Edit `config.yaml` with your PostgreSQL connection details:
   ```yaml
   database:
     host: localhost
     port: 5432
     database: dvdrental
     user: your_username
     password: your_password
   ```

4. **Verify installation**
   ```bash
   python main.py --help
   ```

## Usage

### Basic Commands

#### Check for Missing Values
```bash
python main.py --check-missing
```

#### Detect Duplicates
```bash
python main.py --check-duplicates
```

#### Find Date Gaps
```bash
python main.py --find-gaps
```

#### Check DVD Returns
```bash
python main.py --check-returns
```

#### Prepare Warning Emails
```bash
python main.py --prepare-emails
```

#### Generate Comprehensive Report
```bash
python main.py --generate-report
```

### Advanced Usage

#### Custom Configuration
```bash
python main.py --config custom_config.yaml --check-missing
```

#### Multiple Analysis Types
```bash
python main.py --check-missing --check-duplicates --find-gaps
```

## Configuration

The application uses a YAML configuration file (`config.yaml`) for all settings:

### Database Configuration
```yaml
database:
  host: localhost
  port: 5432
  database: dvdrental
  user: postgres
  password: your_password_here
```

### Analysis Thresholds
```yaml
analysis:
  missing_values:
    critical_threshold: 50  # Percentage
    high_threshold: 20
    medium_threshold: 5
```

### Business Rules
```yaml
business_rules:
  customer:
    required_fields:
      - "first_name"
      - "last_name"
      - "email"
  rental:
    max_rental_duration_days: 14
    grace_period_days: 7
```

## Project Structure

```
dvd_data_checker/
│
├── main.py                 # Main application entry point
├── config.yaml            # Configuration file
├── requirements.txt       # Python dependencies
├── README.md             # This file
│
├── db/                   # Database layer
│   ├── connector.py      # PostgreSQL connection management
│   ├── schema_reader.py  # Database schema analysis
│   └── data_inserter.py  # Test data insertion
│
├── analysis/             # Data analysis modules
│   ├── missing_checker.py    # Missing value detection
│   ├── duplicate_checker.py  # Duplicate record analysis
│   ├── date_gap_finder.py    # Time gap detection
│   └── regression_generator.py # Test data generation
│
├── use_cases/            # Business logic modules
│   ├── dvd_return_check.py   # Overdue rental detection
│   └── email_preparer.py     # Email template generation
│
└── output/               # Generated reports
    └── reports/          # Analysis reports
```

## API Reference

### Database Connector
```python
from db.connector import DatabaseConnector

# Initialize connection
db = DatabaseConnector(config['database'])

# Execute queries
results = db.execute_query_dict("SELECT * FROM customer")
```

### Missing Value Checker
```python
from analysis.missing_checker import MissingValueChecker

checker = MissingValueChecker(db)
results = checker.run_analysis()
report = checker.generate_missing_data_report(results)
```

### DVD Return Checker
```python
from use_cases.dvd_return_check import DVDReturnChecker

checker = DVDReturnChecker(db)
analysis = checker.check_missing_returns()
report = checker.generate_missing_returns_report(analysis)
```

## Reports

The application generates several types of reports:

### Missing Value Report
- Tables with missing data
- Severity classification (Critical/High/Medium/Low)
- Percentage of missing values
- Recommendations for data cleanup

### Duplicate Analysis Report
- Primary key violations
- Business key duplicates
- General duplicate records
- Data integrity recommendations

### DVD Returns Report
- Overdue rental statistics
- Customer segmentation
- Fee calculations
- Action recommendations

### Email Preparation Report
- Number of emails prepared
- Template usage statistics
- Customer contact information
- CSV export for bulk sending

## Testing

### Run Tests
```bash
pytest tests/
```

### Generate Test Data
```python
from analysis.regression_generator import RegressionGenerator

generator = RegressionGenerator(db)
dataset = generator.create_regression_dataset(include_issues=True)
```

### Cleanup Test Data
```python
generator.cleanup_regression_data()
```

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Verify PostgreSQL is running
   - Check connection credentials in `config.yaml`
   - Ensure database exists and is accessible

2. **Permission Errors**
   - Verify database user has appropriate permissions
   - Check file permissions for output directory

3. **Memory Issues**
   - Reduce batch size in configuration
   - Process smaller datasets
   - Increase system memory

### Logging

The application logs all activities to `dvd_data_checker.log`. Set log level in configuration:

```yaml
logging:
  level: "DEBUG"  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Setup
```bash
# Install development dependencies
pip install -r requirements.txt

# Run code formatting
black .
isort .

# Run linting
flake8 .

# Run type checking
mypy .
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the repository
- Check the documentation
- Review the configuration examples

## Roadmap

- [ ] Web interface for analysis results
- [ ] Real-time monitoring dashboard
- [ ] Integration with email services (SMTP)
- [ ] Slack/Teams notifications
- [ ] Advanced data visualization
- [ ] Machine learning for anomaly detection
- [ ] API endpoints for external integration
