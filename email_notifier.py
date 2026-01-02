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
from datetime import datetime


def send_property_results_notification(
    csv_with_distances_path,
    excel_attachment_path=None,
    recipient_email=None,
    test_mode=False,
    property_type='rental',
    type_config=None
):
    """
    Send email notification with file attachments.
    
    First Principles:
    - MIMEMultipart: Creates an email that can have multiple parts (text + attachments)
    - MIMEBase: Base class for email attachments
    - We attach files by reading them, encoding them, and adding to the email
    - smtplib connects to Gmail's SMTP server to send the email
    
    Args:
        csv_with_distances_path (str): Path to property_listings_with_distances.csv
        excel_attachment_path (str, optional): Path to filtered Excel file from data_formatter
        recipient_email (str, optional): Email address to send to (defaults to sender)
        test_mode (bool): If True, skip sending email notification
        property_type (str): 'rental' or 'sales' (default: 'rental' for backward compatibility)
        type_config (dict, optional): Configuration dict for this property type (used for subject prefix)
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    print("\n" + "="*70)
    print("EMAIL NOTIFICATION")
    print("="*70)
    
    # Skip email notification in test mode
    if test_mode:
        print("🧪 TEST MODE: Skipping email notification")
        return True
    
    # Load environment variables
    load_dotenv()
    sender_email = os.getenv('EMAIL')
    sender_password = os.getenv('PASSWORD')
    
    # ============================================
    # VALIDATE CREDENTIALS
    # ============================================
    print("\n📋 Validating email configuration...")
    
    if not sender_email:
        print("❌ Error: EMAIL not set in .env file")
        print("   Please add EMAIL=your-email@gmail.com to your .env file")
        return False
    
    if not sender_password:
        print("❌ Error: PASSWORD not set in .env file")
        print("   Please add PASSWORD=your-app-password to your .env file")
        print("   Note: Use a Gmail App Password, not your regular password")
        return False
    
    # Clean password (remove any non-breaking spaces)
    sender_password = sender_password.replace('\xa0', ' ').strip()
    
    print(f"   Sender: {sender_email}")
    print(f"   Password: {'*' * (len(sender_password) - 4)}{sender_password[-4:]}")
    
    # Default recipient is the sender
    if recipient_email is None:
        recipient_email = sender_email
    
    print(f"   Recipient: {recipient_email}")
    
    # ============================================
    # VALIDATE FILE
    # ============================================
    print("\n📋 Validating attachment file...")
    
    if not csv_with_distances_path:
        print("❌ Error: No file path provided")
        return False
    
    if not os.path.exists(csv_with_distances_path):
        print(f"❌ Error: File not found: {csv_with_distances_path}")
        print(f"   Current working directory: {os.getcwd()}")
        print(f"   Absolute path: {os.path.abspath(csv_with_distances_path)}")
        return False
    
    file_size = os.path.getsize(csv_with_distances_path)
    print(f"   File: {csv_with_distances_path}")
    print(f"   Size: {file_size:,} bytes")
    
    # Count properties in the file (for the email message)
    count_all = "N/A"
    count_completed = "N/A"
    try:
        import pandas as pd
        df_all = pd.read_csv(csv_with_distances_path)
        count_all = len(df_all)
        
        # Count completed properties if column exists
        if 'processing_status' in df_all.columns:
            count_completed = len(df_all[df_all['processing_status'] == 'completed'])
        else:
            count_completed = count_all  # Assume all are completed if no status column
            
        print(f"   Properties: {count_all} (completed: {count_completed})")
    except Exception as e:
        print(f"⚠️  Warning: Could not read CSV file to count properties: {e}")
    
    # ============================================
    # CREATE EMAIL MESSAGE
    # ============================================
    print("\n📧 Creating email message...")
    
    # Determine subject prefix from type_config or use default
    if type_config and 'email' in type_config and 'subject_prefix' in type_config['email']:
        subject_prefix = type_config['email']['subject_prefix']
    else:
        # Default based on property type
        if property_type == 'sales':
            subject_prefix = 'New Sales Matches'
        else:
            subject_prefix = 'New Rental Matches'
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = f"🏠 {subject_prefix}: {count_all} Properties Ready! ({datetime.now().strftime('%Y-%m-%d')})"
    
    # Check if Excel file exists
    has_excel = excel_attachment_path and os.path.exists(excel_attachment_path)
    
    # Build file list for email body (use actual filenames from paths)
    csv_filename = os.path.basename(csv_with_distances_path)
    files_list = f"""<li>{csv_filename} - All completed properties with distance calculations and nearby gyms</li>"""
    if has_excel:
        excel_filename = os.path.basename(excel_attachment_path)
        files_list += f"""<li>{excel_filename} - <strong>Filtered properties</strong> based on your custom criteria (Excel format with formatting)</li>"""
    
    # Create email body
    body = f"""
    <html>
      <body>
        <h2>Property Finder Results Ready!</h2>
        <p>Your property search has completed successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}.</p>
        
        <h3>Summary:</h3>
        <ul>
          <li><strong>All completed properties:</strong> {count_all} properties</li>
        </ul>
        
        <p>{'The files are' if has_excel else 'The file is'} attached to this email. Download and open to view the results.</p>
        
        <p>Files attached:</p>
        <ul>
          {files_list}
        </ul>
        
        <p>Best regards,<br>Property Finder Automation</p>
      </body>
    </html>
    """
    
    # Attach the HTML body
    msg.attach(MIMEText(body, 'html'))
    
    # Attach the CSV file (use actual filename from path)
    try:
        csv_attachment_name = os.path.basename(csv_with_distances_path)
        attach_file(msg, csv_with_distances_path, csv_attachment_name)
        print("   ✅ CSV file attached successfully")
    except Exception as e:
        print(f"❌ Error attaching CSV file: {e}")
        return False
    
    # Attach the Excel file (if provided and exists)
    if has_excel:
        try:
            excel_filename = os.path.basename(excel_attachment_path)
            attach_file(msg, excel_attachment_path, excel_filename)
            print(f"   ✅ Excel file attached successfully ({excel_filename})")
        except Exception as e:
            print(f"⚠️  Warning: Could not attach Excel file: {e}")
            # Continue without Excel attachment - not a fatal error
    
    # ============================================
    # SEND EMAIL
    # ============================================
    print("\n📤 Sending email...")
    
    try:
        # Connect to Gmail's SMTP server
        print("   Connecting to smtp.gmail.com:587...")
        server = smtplib.SMTP('smtp.gmail.com', 587, timeout=30)
        server.set_debuglevel(0)  # Set to 1 for verbose SMTP output
        
        print("   Starting TLS encryption...")
        server.starttls()
        
        print("   Logging in...")
        server.login(sender_email, sender_password)
        
        print("   Sending message...")
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        
        print("   Closing connection...")
        server.quit()
        
        print(f"\n✅ Email sent successfully to {recipient_email}!")
        print("="*70)
        return True
        
    except smtplib.SMTPAuthenticationError as e:
        print(f"\n❌ Error: Authentication failed")
        print(f"   Details: {e}")
        print("   Possible causes:")
        print("   - Wrong email or password in .env file")
        print("   - Not using a Gmail App Password (required for Gmail)")
        print("   - 2-factor authentication not enabled on Gmail account")
        print("   To create an App Password:")
        print("   1. Go to https://myaccount.google.com/apppasswords")
        print("   2. Generate a new app password for 'Mail'")
        print("   3. Update PASSWORD in your .env file")
        print("="*70)
        return False
    except smtplib.SMTPConnectError as e:
        print(f"\n❌ Error: Could not connect to SMTP server")
        print(f"   Details: {e}")
        print("   Check your internet connection and firewall settings")
        print("="*70)
        return False
    except smtplib.SMTPServerDisconnected as e:
        print(f"\n❌ Error: Server disconnected unexpectedly")
        print(f"   Details: {e}")
        print("="*70)
        return False
    except TimeoutError as e:
        print(f"\n❌ Error: Connection timed out")
        print(f"   Details: {e}")
        print("   Check your internet connection")
        print("="*70)
        return False
    except Exception as e:
        print(f"\n❌ Error sending email: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print("="*70)
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