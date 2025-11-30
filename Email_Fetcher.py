import imaplib
import ssl
import email

# Your Gmail credentials
EMAIL = 'isurueiendom@gmail.com'  # Replace with your actual email
PASSWORD = 'pzjq esze cfmi vxvx'    # Replace with your app password (or regular password if no 2FA)

# Normalize to replace any non-breaking spaces with regular spaces
PASSWORD = PASSWORD.replace('\xa0', ' ').strip()

# Connect to the IMAP server
context = ssl.create_default_context()
mail = imaplib.IMAP4_SSL('imap.gmail.com', port=993, ssl_context=context)

# Log in
mail.login(EMAIL, PASSWORD)

# Select the inbox
mail.select('INBOX')

# If we get here, connection is successful
print("Successfully connected to inbox!")

# Search for recent emails: 'ALL' gets everything, but limit to last 1 for simplicity
status, messages = mail.search(None, 'ALL')  # Returns a space-separated string of IDs
if status != 'OK':
    print("No messages found!")
else:
    # Get the list of email IDs and take the last one (most recent)
    email_ids = messages[0].split(b' ')  # Split into list
    latest_email_id = email_ids[-1]  # -1 is the last item (newest in ascending order)

    # Fetch the header of the latest email (RFC822 format for full headers)
    status, msg_data = mail.fetch(latest_email_id, '(RFC822.HEADER)')
    if status == 'OK':
        # Parse the raw header bytes into an email.message object
        raw_header = msg_data[0][1]  # msg_data is a list of tuples; [0][1] is the bytes
        msg = email.message_from_bytes(raw_header)

        # Extract and print the subject (handles decoding if needed)
        subject = msg['Subject']
        print("Subject of the most recent email:", subject)
    else:
        print("Failed to fetch email!")

# Logout
mail.logout()

