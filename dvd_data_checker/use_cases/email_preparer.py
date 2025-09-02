"""
Email Preparer Use Case
Prepares warning emails for customers with overdue rentals and data quality issues.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from db.connector import DatabaseConnector


class EmailPreparer:
    """Prepares warning emails for customers with overdue rentals and data issues."""
    
    def __init__(self, db_connector: DatabaseConnector):
        """
        Initialize email preparer with database connector.
        
        Args:
            db_connector: DatabaseConnector instance
        """
        self.db = db_connector
        self.logger = logging.getLogger(__name__)
        
        # Email templates
        self.email_templates = {
            'overdue_reminder': {
                'subject': 'DVD Rental Overdue - Action Required',
                'template': """
Dear {first_name} {last_name},

We hope this email finds you well. We noticed that you have the following DVD rental(s) that are currently overdue:

{overdue_items}

Please return these items as soon as possible to avoid additional late fees. Current overdue fees: ${total_fees}

If you have already returned these items, please disregard this message.

Thank you for your prompt attention to this matter.

Best regards,
DVD Rental Store Team
                """
            },
            'multiple_overdue_warning': {
                'subject': 'Multiple Overdue Rentals - Urgent Action Required',
                'template': """
Dear {first_name} {last_name},

We are writing to inform you that you currently have {overdue_count} overdue DVD rental(s) in your account:

{overdue_items}

Total overdue fees: ${total_fees}

This is a serious matter that requires immediate attention. Please return all overdue items within 48 hours to avoid:
- Additional late fees
- Potential account suspension
- Collection procedures

If you have any questions or need to make arrangements, please contact us immediately.

Best regards,
DVD Rental Store Team
                """
            },
            'critical_overdue_final': {
                'subject': 'FINAL NOTICE - Overdue Rentals Requiring Immediate Action',
                'template': """
Dear {first_name} {last_name},

This is our FINAL NOTICE regarding your overdue DVD rental(s):

{overdue_items}

Total overdue fees: ${total_fees}
Days overdue: {max_days_overdue}

Your account is now at risk of suspension and these items may be reported as lost. To avoid this:

1. Return all items immediately
2. Pay all outstanding fees
3. Contact us to discuss payment arrangements

If we do not hear from you within 7 days, we will proceed with account suspension and collection procedures.

Best regards,
DVD Rental Store Team
                """
            },
            'data_quality_issue': {
                'subject': 'Account Information Update Required',
                'template': """
Dear {first_name} {last_name},

We noticed that some information in your account needs to be updated:

{missing_fields}

Please log into your account and update this information to ensure uninterrupted service.

If you have any questions, please contact our customer service team.

Best regards,
DVD Rental Store Team
                """
            }
        }
    
    def prepare_overdue_emails(self, days_overdue: int = 7) -> List[Dict[str, Any]]:
        """
        Prepare emails for customers with overdue rentals.
        
        Args:
            days_overdue: Number of days after which a rental is considered overdue
            
        Returns:
            List of email data dictionaries
        """
        try:
            # Get overdue rentals
            overdue_date = datetime.now() - timedelta(days=days_overdue)
            
            query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                COUNT(r.rental_id) as overdue_count,
                MAX(EXTRACT(DAY FROM (NOW() - r.rental_date))) as max_days_overdue,
                STRING_AGG(f.title, ', ') as overdue_titles
            FROM customer c
            JOIN rental r ON c.customer_id = r.customer_id
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.return_date IS NULL
                AND r.rental_date < %s
                AND c.email IS NOT NULL
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email
            ORDER BY max_days_overdue DESC
            """
            
            overdue_customers = self.db.execute_query_dict(query, (overdue_date,))
            
            emails = []
            for customer in overdue_customers:
                # Calculate fees
                fee_info = self._calculate_customer_fees(customer['customer_id'])
                
                # Determine email template based on severity
                if customer['max_days_overdue'] > 30:
                    template_type = 'critical_overdue_final'
                elif customer['overdue_count'] > 2:
                    template_type = 'multiple_overdue_warning'
                else:
                    template_type = 'overdue_reminder'
                
                # Prepare email content
                email_data = self._prepare_email_content(
                    customer, template_type, fee_info
                )
                
                emails.append(email_data)
            
            self.logger.info(f"Prepared {len(emails)} overdue reminder emails")
            return emails
            
        except Exception as e:
            self.logger.error(f"Error preparing overdue emails: {e}")
            return []
    
    def prepare_data_quality_emails(self) -> List[Dict[str, Any]]:
        """
        Prepare emails for customers with data quality issues.
        
        Returns:
            List of email data dictionaries
        """
        try:
            # Find customers with missing critical data
            query = """
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                CASE 
                    WHEN email IS NULL THEN 'Email address'
                    ELSE NULL
                END as missing_email,
                CASE 
                    WHEN first_name IS NULL THEN 'First name'
                    ELSE NULL
                END as missing_first_name,
                CASE 
                    WHEN last_name IS NULL THEN 'Last name'
                    ELSE NULL
                END as missing_last_name
            FROM customer
            WHERE (email IS NULL OR first_name IS NULL OR last_name IS NULL)
                AND email IS NOT NULL  -- Only send to customers with valid emails
            """
            
            customers_with_issues = self.db.execute_query_dict(query)
            
            emails = []
            for customer in customers_with_issues:
                # Determine missing fields
                missing_fields = []
                if customer['missing_email']:
                    missing_fields.append(customer['missing_email'])
                if customer['missing_first_name']:
                    missing_fields.append(customer['missing_first_name'])
                if customer['missing_last_name']:
                    missing_fields.append(customer['missing_last_name'])
                
                if missing_fields:
                    # Prepare email content
                    email_data = self._prepare_data_quality_email(
                        customer, missing_fields
                    )
                    emails.append(email_data)
            
            self.logger.info(f"Prepared {len(emails)} data quality emails")
            return emails
            
        except Exception as e:
            self.logger.error(f"Error preparing data quality emails: {e}")
            return []
    
    def prepare_warning_emails(self) -> Dict[str, Any]:
        """
        Prepare all warning emails (overdue and data quality).
        
        Returns:
            Dictionary with email preparation results
        """
        try:
            self.logger.info("Starting email preparation process")
            
            # Prepare overdue emails
            overdue_emails = self.prepare_overdue_emails()
            
            # Prepare data quality emails
            data_quality_emails = self.prepare_data_quality_emails()
            
            # Combine results
            total_emails = len(overdue_emails) + len(data_quality_emails)
            
            results = {
                'total_emails_prepared': total_emails,
                'overdue_emails': len(overdue_emails),
                'data_quality_emails': len(data_quality_emails),
                'overdue_email_details': overdue_emails,
                'data_quality_email_details': data_quality_emails,
                'preparation_date': datetime.now()
            }
            
            self.logger.info(f"Email preparation complete: {total_emails} emails prepared")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in email preparation: {e}")
            return {
                'error': str(e),
                'total_emails_prepared': 0,
                'overdue_emails': 0,
                'data_quality_emails': 0
            }
    
    def _calculate_customer_fees(self, customer_id: int) -> Dict[str, Any]:
        """
        Calculate overdue fees for a customer.
        
        Args:
            customer_id: Customer ID to calculate fees for
            
        Returns:
            Dictionary with fee information
        """
        try:
            query = """
            SELECT 
                SUM(CASE 
                    WHEN EXTRACT(DAY FROM (NOW() - r.rental_date)) <= 7 THEN 0
                    WHEN EXTRACT(DAY FROM (NOW() - r.rental_date)) <= 14 THEN f.rental_rate * 0.5 * (EXTRACT(DAY FROM (NOW() - r.rental_date)) - 7)
                    ELSE f.rental_rate * (EXTRACT(DAY FROM (NOW() - r.rental_date)) - 7)
                END) as total_fees
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.customer_id = %s
                AND r.return_date IS NULL
                AND r.rental_date < (NOW() - INTERVAL '7 days')
            """
            
            result = self.db.execute_query_dict(query, (customer_id,))
            
            if result and result[0]['total_fees']:
                return {
                    'total_fees': round(float(result[0]['total_fees']), 2),
                    'has_fees': True
                }
            else:
                return {
                    'total_fees': 0.00,
                    'has_fees': False
                }
                
        except Exception as e:
            self.logger.error(f"Error calculating fees for customer {customer_id}: {e}")
            return {
                'total_fees': 0.00,
                'has_fees': False
            }
    
    def _prepare_email_content(self, customer: Dict[str, Any], template_type: str, 
                              fee_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare email content for a customer.
        
        Args:
            customer: Customer data dictionary
            template_type: Type of email template to use
            fee_info: Fee calculation information
            
        Returns:
            Dictionary with email content
        """
        template = self.email_templates[template_type]
        
        # Prepare overdue items list
        overdue_items = []
        if customer['overdue_titles']:
            titles = customer['overdue_titles'].split(', ')
            for title in titles:
                overdue_items.append(f"- {title}")
        
        overdue_items_text = "\n".join(overdue_items) if overdue_items else "No specific titles available"
        
        # Format email content
        content = template['template'].format(
            first_name=customer['first_name'] or 'Valued Customer',
            last_name=customer['last_name'] or '',
            overdue_items=overdue_items_text,
            total_fees=fee_info['total_fees'],
            overdue_count=customer['overdue_count'],
            max_days_overdue=customer['max_days_overdue']
        )
        
        return {
            'customer_id': customer['customer_id'],
            'email': customer['email'],
            'subject': template['subject'],
            'content': content.strip(),
            'template_type': template_type,
            'overdue_count': customer['overdue_count'],
            'total_fees': fee_info['total_fees'],
            'max_days_overdue': customer['max_days_overdue']
        }
    
    def _prepare_data_quality_email(self, customer: Dict[str, Any], 
                                   missing_fields: List[str]) -> Dict[str, Any]:
        """
        Prepare data quality email for a customer.
        
        Args:
            customer: Customer data dictionary
            missing_fields: List of missing field names
            
        Returns:
            Dictionary with email content
        """
        template = self.email_templates['data_quality_issue']
        
        # Format missing fields list
        missing_fields_text = "\n".join([f"- {field}" for field in missing_fields])
        
        # Format email content
        content = template['template'].format(
            first_name=customer['first_name'] or 'Valued Customer',
            last_name=customer['last_name'] or '',
            missing_fields=missing_fields_text
        )
        
        return {
            'customer_id': customer['customer_id'],
            'email': customer['email'],
            'subject': template['subject'],
            'content': content.strip(),
            'template_type': 'data_quality_issue',
            'missing_fields': missing_fields
        }
    
    def generate_email_report(self, email_results: Dict[str, Any]) -> str:
        """
        Generate a report of prepared emails.
        
        Args:
            email_results: Results from email preparation
            
        Returns:
            Formatted report string
        """
        if 'error' in email_results:
            return f"ERROR: {email_results['error']}"
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("EMAIL PREPARATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Summary
        report_lines.append("SUMMARY")
        report_lines.append("-" * 40)
        report_lines.append(f"Total emails prepared: {email_results['total_emails_prepared']}")
        report_lines.append(f"Overdue reminder emails: {email_results['overdue_emails']}")
        report_lines.append(f"Data quality emails: {email_results['data_quality_emails']}")
        report_lines.append("")
        
        # Overdue email details
        if email_results['overdue_emails'] > 0:
            report_lines.append("OVERDUE EMAIL DETAILS")
            report_lines.append("-" * 40)
            
            # Group by template type
            template_counts = {}
            total_fees = 0
            
            for email in email_results['overdue_email_details']:
                template_type = email['template_type']
                template_counts[template_type] = template_counts.get(template_type, 0) + 1
                total_fees += email['total_fees']
            
            for template_type, count in template_counts.items():
                report_lines.append(f"  {template_type}: {count} emails")
            
            report_lines.append(f"  Total potential fees: ${total_fees}")
            report_lines.append("")
        
        # Data quality email details
        if email_results['data_quality_emails'] > 0:
            report_lines.append("DATA QUALITY EMAIL DETAILS")
            report_lines.append("-" * 40)
            
            # Group by missing field type
            field_counts = {}
            for email in email_results['data_quality_email_details']:
                for field in email['missing_fields']:
                    field_counts[field] = field_counts.get(field, 0) + 1
            
            for field, count in field_counts.items():
                report_lines.append(f"  Missing {field}: {count} customers")
            report_lines.append("")
        
        # Recommendations
        report_lines.append("RECOMMENDATIONS")
        report_lines.append("-" * 40)
        
        if email_results['overdue_emails'] > 0:
            report_lines.append("📧 OVERDUE EMAILS:")
            report_lines.append("  - Send emails immediately to reduce overdue items")
            report_lines.append("  - Follow up with phone calls for critical overdue")
            report_lines.append("  - Consider automated email scheduling")
        
        if email_results['data_quality_emails'] > 0:
            report_lines.append("📧 DATA QUALITY EMAILS:")
            report_lines.append("  - Send emails to improve customer data")
            report_lines.append("  - Consider incentives for data updates")
            report_lines.append("  - Implement data validation on signup")
        
        report_lines.append("")
        report_lines.append(f"Report generated: {email_results['preparation_date']}")
        
        return "\n".join(report_lines)
    
    def export_emails_to_csv(self, email_results: Dict[str, Any], filename: str = None) -> str:
        """
        Export prepared emails to CSV format.
        
        Args:
            email_results: Results from email preparation
            filename: Output filename (optional)
            
        Returns:
            CSV content string
        """
        if 'error' in email_results:
            return f"ERROR: {email_results['error']}"
        
        if filename is None:
            filename = f"prepared_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        csv_lines = []
        csv_lines.append("customer_id,email,subject,template_type,overdue_count,total_fees,max_days_overdue")
        
        # Add overdue emails
        for email in email_results.get('overdue_email_details', []):
            csv_lines.append(f"{email['customer_id']},{email['email']},{email['subject']},{email['template_type']},{email['overdue_count']},{email['total_fees']},{email['max_days_overdue']}")
        
        # Add data quality emails
        for email in email_results.get('data_quality_email_details', []):
            csv_lines.append(f"{email['customer_id']},{email['email']},{email['subject']},{email['template_type']},0,0.00,0")
        
        csv_content = "\n".join(csv_lines)
        
        # Save to file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            self.logger.info(f"Emails exported to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving CSV file: {e}")
        
        return csv_content
