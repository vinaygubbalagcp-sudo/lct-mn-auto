
import re

# Email validation regex pattern
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

def is_valid_email(email_id):
    """
    Validates the given email ID using regex.
    Returns True if valid, else False.
    """
    if re.match(EMAIL_PATTERN, email_id):
        return True
    return False


# Example email list (replace with your 30 ids)
emails = [
    "test@gmail.com",
    "invalid_email@",
    "user.name123@company.co.in",
    "wrong@domain",
]

# Validate each
for email in emails:
    print(email, "=>", is_valid_email(email))

