"""
DVD Return Checker Use Case
Finds customers with missing DVD returns and overdue rentals.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector


class DVDReturnChecker:
    """Checks for missing DVD returns and overdue rentals."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize DVD return checker with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.logger = logging.getLogger(__name__)
    
    def find_missing_returns(self, days_overdue: int = 7) -> List[Dict[str, Any]]:
        """
        Find rentals with missing return dates that are overdue.
        
        Args:
            days_overdue: Number of days after which a rental is considered overdue
            
        Returns:
            List of overdue rental records
        """
        try:
            overdue_date = datetime.now() - timedelta(days=days_overdue)
            
            query = """
            SELECT 
                r.rental_id,
                r.rental_date,
                r.customer_id,
                r.inventory_id,
                r.staff_id,
                c.first_name,
                c.last_name,
                c.email,
                f.title as film_title,
                EXTRACT(DAY FROM (NOW() - r.rental_date)) as days_overdue
            FROM rental r
            JOIN customer c ON r.customer_id = c.customer_id
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.return_date IS NULL
                AND r.rental_date < %s
            ORDER BY r.rental_date ASC
            """
            
            overdue_rentals = self.db.execute_query_dict(query, (overdue_date,))
            
            self.logger.info(f"Found {len(overdue_rentals)} overdue rentals")
            return overdue_rentals
            
        except Exception as e:
            self.logger.error(f"Error finding missing returns: {e}")
            return []
    
    def find_customers_with_multiple_overdue(self, min_overdue: int = 2) -> List[Dict[str, Any]]:
        """
        Find customers with multiple overdue rentals.
        
        Args:
            min_overdue: Minimum number of overdue rentals to flag
            
        Returns:
            List of customers with multiple overdue rentals
        """
        try:
            query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                COUNT(r.rental_id) as overdue_count,
                MAX(r.rental_date) as latest_rental,
                MIN(r.rental_date) as earliest_rental,
                AVG(EXTRACT(DAY FROM (NOW() - r.rental_date))) as avg_days_overdue
            FROM customer c
            JOIN rental r ON c.customer_id = r.customer_id
            WHERE r.return_date IS NULL
                AND r.rental_date < (NOW() - INTERVAL '7 days')
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email
            HAVING COUNT(r.rental_id) >= %s
            ORDER BY overdue_count DESC, avg_days_overdue DESC
            """
            
            customers = self.db.execute_query_dict(query, (min_overdue,))
            
            self.logger.info(f"Found {len(customers)} customers with {min_overdue}+ overdue rentals")
            return customers
            
        except Exception as e:
            self.logger.error(f"Error finding customers with multiple overdue: {e}")
            return []
    
    def get_rental_history(self, customer_id: int, days_back: int = 90) -> List[Dict[str, Any]]:
        """
        Get rental history for a specific customer.
        
        Args:
            customer_id: Customer ID to check
            days_back: Number of days to look back
            
        Returns:
            List of rental history records
        """
        try:
            start_date = datetime.now() - timedelta(days=days_back)
            
            query = """
            SELECT 
                r.rental_id,
                r.rental_date,
                r.return_date,
                f.title as film_title,
                CASE 
                    WHEN r.return_date IS NULL THEN 'Overdue'
                    WHEN r.return_date > (r.rental_date + INTERVAL '14 days') THEN 'Late Return'
                    ELSE 'On Time'
                END as return_status,
                EXTRACT(DAY FROM (COALESCE(r.return_date, NOW()) - r.rental_date)) as rental_duration
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.customer_id = %s
                AND r.rental_date >= %s
            ORDER BY r.rental_date DESC
            """
            
            history = self.db.execute_query_dict(query, (customer_id, start_date))
            
            return history
            
        except Exception as e:
            self.logger.error(f"Error getting rental history for customer {customer_id}: {e}")
            return []
    
    def calculate_overdue_fees(self, customer_id: int) -> Dict[str, Any]:
        """
        Calculate overdue fees for a customer.
        
        Args:
            customer_id: Customer ID to calculate fees for
            
        Returns:
            Dictionary with fee calculation details
        """
        try:
            query = """
            SELECT 
                r.rental_id,
                r.rental_date,
                f.rental_rate,
                f.replacement_cost,
                EXTRACT(DAY FROM (NOW() - r.rental_date)) as days_overdue,
                CASE 
                    WHEN EXTRACT(DAY FROM (NOW() - r.rental_date)) <= 7 THEN 0
                    WHEN EXTRACT(DAY FROM (NOW() - r.rental_date)) <= 14 THEN f.rental_rate * 0.5
                    ELSE f.rental_rate
                END as daily_fee
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.customer_id = %s
                AND r.return_date IS NULL
                AND r.rental_date < (NOW() - INTERVAL '7 days')
            """
            
            overdue_items = self.db.execute_query_dict(query, (customer_id,))
            
            total_fees = 0
            total_items = len(overdue_items)
            max_days_overdue = 0
            
            for item in overdue_items:
                days_overdue = item['days_overdue']
                daily_fee = item['daily_fee']
                item_fee = daily_fee * max(0, days_overdue - 7)  # Fee starts after 7 days
                total_fees += item_fee
                max_days_overdue = max(max_days_overdue, days_overdue)
            
            return {
                'customer_id': customer_id,
                'total_fees': round(total_fees, 2),
                'total_items': total_items,
                'max_days_overdue': max_days_overdue,
                'overdue_items': overdue_items
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating fees for customer {customer_id}: {e}")
            return {
                'customer_id': customer_id,
                'total_fees': 0,
                'total_items': 0,
                'max_days_overdue': 0,
                'overdue_items': []
            }
    
    def check_missing_returns(self, days_overdue: int = 7) -> Dict[str, Any]:
        """
        Comprehensive check for missing returns and overdue rentals.
        
        Args:
            days_overdue: Number of days after which a rental is considered overdue
            
        Returns:
            Dictionary with comprehensive missing returns analysis
        """
        try:
            self.logger.info("Starting comprehensive missing returns check")
            
            # Find all overdue rentals
            overdue_rentals = self.find_missing_returns(days_overdue)
            
            # Find customers with multiple overdue
            multiple_overdue = self.find_customers_with_multiple_overdue(2)
            
            # Calculate statistics
            total_overdue = len(overdue_rentals)
            total_customers = len(set(rental['customer_id'] for rental in overdue_rentals))
            total_fees = 0
            
            # Calculate fees for each customer
            customer_fees = {}
            for rental in overdue_rentals:
                customer_id = rental['customer_id']
                if customer_id not in customer_fees:
                    fee_info = self.calculate_overdue_fees(customer_id)
                    customer_fees[customer_id] = fee_info
                    total_fees += fee_info['total_fees']
            
            # Categorize by severity
            critical_overdue = [r for r in overdue_rentals if r['days_overdue'] > 30]
            high_overdue = [r for r in overdue_rentals if 15 < r['days_overdue'] <= 30]
            medium_overdue = [r for r in overdue_rentals if 7 < r['days_overdue'] <= 15]
            
            analysis = {
                'total_overdue_rentals': total_overdue,
                'total_customers_affected': total_customers,
                'total_potential_fees': round(total_fees, 2),
                'severity_breakdown': {
                    'critical': len(critical_overdue),
                    'high': len(high_overdue),
                    'medium': len(medium_overdue)
                },
                'customers_multiple_overdue': len(multiple_overdue),
                'overdue_rentals': overdue_rentals,
                'multiple_overdue_customers': multiple_overdue,
                'customer_fees': customer_fees,
                'analysis_date': datetime.now()
            }
            
            self.logger.info(f"Missing returns analysis complete: {total_overdue} overdue rentals found")
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive missing returns check: {e}")
            return {
                'error': str(e),
                'total_overdue_rentals': 0,
                'total_customers_affected': 0,
                'total_potential_fees': 0
            }
    
    def generate_missing_returns_report(self, analysis: Dict[str, Any]) -> str:
        """
        Generate a human-readable report from missing returns analysis.
        
        Args:
            analysis: Results from missing returns analysis
            
        Returns:
            Formatted report string
        """
        if 'error' in analysis:
            return f"ERROR: {analysis['error']}"
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("MISSING DVD RETURNS ANALYSIS REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Summary statistics
        report_lines.append("SUMMARY STATISTICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Total overdue rentals: {analysis['total_overdue_rentals']}")
        report_lines.append(f"Customers affected: {analysis['total_customers_affected']}")
        report_lines.append(f"Total potential fees: ${analysis['total_potential_fees']}")
        report_lines.append("")
        
        # Severity breakdown
        severity = analysis['severity_breakdown']
        report_lines.append("SEVERITY BREAKDOWN")
        report_lines.append("-" * 40)
        report_lines.append(f"Critical (>30 days): {severity['critical']}")
        report_lines.append(f"High (15-30 days): {severity['high']}")
        report_lines.append(f"Medium (7-15 days): {severity['medium']}")
        report_lines.append("")
        
        # Customers with multiple overdue
        if analysis['customers_multiple_overdue'] > 0:
            report_lines.append("CUSTOMERS WITH MULTIPLE OVERDUE RENTALS")
            report_lines.append("-" * 40)
            for customer in analysis['multiple_overdue_customers'][:10]:  # Show top 10
                report_lines.append(f"  {customer['first_name']} {customer['last_name']} ({customer['email']})")
                report_lines.append(f"    Overdue rentals: {customer['overdue_count']}")
                report_lines.append(f"    Average days overdue: {customer['avg_days_overdue']:.1f}")
                report_lines.append("")
        
        # Top overdue rentals
        if analysis['overdue_rentals']:
            report_lines.append("TOP OVERDUE RENTALS")
            report_lines.append("-" * 40)
            for rental in analysis['overdue_rentals'][:10]:  # Show top 10
                report_lines.append(f"  {rental['film_title']} - {rental['first_name']} {rental['last_name']}")
                report_lines.append(f"    Rental date: {rental['rental_date']}")
                report_lines.append(f"    Days overdue: {rental['days_overdue']}")
                report_lines.append("")
        
        # Recommendations
        report_lines.append("RECOMMENDATIONS")
        report_lines.append("-" * 40)
        if severity['critical'] > 0:
            report_lines.append("🚨 IMMEDIATE ACTION REQUIRED:")
            report_lines.append("  - Contact customers with rentals overdue >30 days")
            report_lines.append("  - Consider collection procedures for long-overdue items")
        
        if severity['high'] > 0:
            report_lines.append("⚠️  HIGH PRIORITY:")
            report_lines.append("  - Send reminder emails to customers with 15-30 day overdue")
            report_lines.append("  - Follow up with phone calls for multiple overdue customers")
        
        if analysis['total_potential_fees'] > 100:
            report_lines.append("💰 REVENUE OPPORTUNITY:")
            report_lines.append(f"  - Potential fee collection: ${analysis['total_potential_fees']}")
            report_lines.append("  - Implement automated fee calculation and billing")
        
        report_lines.append("")
        report_lines.append(f"Report generated: {analysis['analysis_date']}")
        
        return "\n".join(report_lines)
