# email_notifier.py
# Email notification module for Property Finder
# Sends notification email with CSV file attachments

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from pathlib import Path


def send_property_results_notification(
    csv_with_distances_path,
    csv_filtered_path,
    recipient_email=None,
    test_mode=False
):
    """
    Send email notification with CSV file attachments.
    
    First Principles:
    - MIMEMultipart: Creates an email that can have multiple parts (text + attachments)
    - MIMEBase: Base class for email attachments
    - We attach files by reading them, encoding them, and adding to the email
    - smtplib connects to Gmail's SMTP server to send the email
    
    Args:
        csv_with_distances_path (str): Path to property_listings_with_distances.csv
        csv_filtered_path (str): Path to property_listings_filtered_by_distance.csv
        recipient_email (str, optional): Email address to send to (defaults to sender)
        test_mode (bool): If True, skip sending email notification
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    # Skip email notification in test mode
    if test_mode:
        print("🧪 TEST MODE: Skipping email notification")
        return True
    
    # Load environment variables
    load_dotenv()
    sender_email = os.getenv('EMAIL')
    sender_password = os.getenv('PASSWORD')
    
    # Clean password (remove any non-breaking spaces)
    if sender_password:
        sender_password = sender_password.replace('\xa0', ' ').strip()
    
    # Default recipient is the sender
    if recipient_email is None:
        recipient_email = sender_email
    
    # Validate that files exist
    if not os.path.exists(csv_with_distances_path):
        print(f"❌ Error: File not found: {csv_with_distances_path}")
        return False
    
    if not os.path.exists(csv_filtered_path):
        print(f"❌ Error: File not found: {csv_filtered_path}")
        return False
    
    # Count properties in each file (for the email message)
    try:
        import pandas as pd
        df_all = pd.read_csv(csv_with_distances_path)
        df_filtered = pd.read_csv(csv_filtered_path)
        count_all = len(df_all)
        count_filtered = len(df_filtered)
    except Exception as e:
        print(f"⚠️  Warning: Could not read CSV files to count properties: {e}")
        count_all = "N/A"
        count_filtered = "N/A"
    
    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "🏠 Property Finder: Results Ready!"
    
    # Create email body
    body = f"""
    <html>
      <body>
        <h2>Property Finder Results Ready!</h2>
        <p>Your property search has completed successfully.</p>
        
        <h3>Summary:</h3>
        <ul>
          <li><strong>All properties with distances:</strong> {count_all} properties</li>
          <li><strong>Filtered properties (within travel time):</strong> {count_filtered} properties</li>
        </ul>
        
        <p>The CSV files are attached to this email. Download and open them to view the results.</p>
        
        <p>Files attached:</p>
        <ul>
          <li>property_listings_with_distances.csv - All properties with distance calculations</li>
          <li>property_listings_filtered_by_distance.csv - Properties within your max travel time</li>
        </ul>
        
        <p>Best regards,<br>Property Finder Automation</p>
      </body>
    </html>
    """
    
    # Attach the HTML body
    msg.attach(MIMEText(body, 'html'))
    
    # Attach the first CSV file
    attach_file(msg, csv_with_distances_path, "property_listings_with_distances.csv")
    
    # Attach the second CSV file
    attach_file(msg, csv_filtered_path, "property_listings_filtered_by_distance.csv")
    
    # Send the email
    try:
        print(f"\n📧 Sending email notification to {recipient_email}...")
        
        # Connect to Gmail's SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Enable encryption
        server.login(sender_email, sender_password)
        
        # Send the email
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print(f"✅ Email sent successfully!")
        return True
        
    except smtplib.SMTPAuthenticationError:
        print("❌ Error: Authentication failed. Check your email and password in .env")
        print("   Make sure you're using a Gmail App Password, not your regular password.")
        return False
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        import traceback
        traceback.print_exc()
        return False


def attach_file(msg, file_path, filename=None):
    """
    Attach a file to an email message.
    
    First Principles:
    - We read the file in binary mode ('rb')
    - Create a MIMEBase object to represent the attachment
    - Encode it using base64 encoding (standard for email attachments)
    - Set the appropriate headers so email clients know it's an attachment
    - Add it to the email message
    
    Args:
        msg: MIMEMultipart message object
        file_path (str): Path to the file to attach
        filename (str, optional): Name for the attachment (defaults to file name)
    """
    if filename is None:
        filename = os.path.basename(file_path)
    
    # Open the file in binary mode
    with open(file_path, 'rb') as attachment:
        # Create a MIMEBase object
        part = MIMEBase('application', 'octet-stream')
        # Read the file content
        part.set_payload(attachment.read())
    
    # Encode the attachment in base64
    encoders.encode_base64(part)
    
    # Add header to indicate it's an attachment
    part.add_header(
        'Content-Disposition',
        f'attachment; filename= {filename}'
    )
    
    # Attach the part to the message
    msg.attach(part)