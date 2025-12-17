# need code for reding data from two csv files and filtering the data basing on the current date
import pandas as pd
from datetime import datetime
# for sending email notification use the following credentials
import smtplib
SENDER_EMAIL = "letsmailrahul27@gmail.com"        # 🔴 change this
SENDER_PASSWORD = "pkpd oqsi sfjj iwyo"   # 🔴 change this app password

def read_and_filter_data(file1, file2):
    # Read data from CSV files
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    
    # Get current date
    current_date = datetime.now().date()
    
    # Convert date columns to datetime
    df1['date'] = pd.to_datetime(df1['date']).dt.date
    df2['date'] = pd.to_datetime(df2['date']).dt.date
    
    # Filter data based on current date
    filtered_df1 = df1[df1['date'] == current_date]
    filtered_df2 = df2[df2['date'] == current_date]
    
    return filtered_df1, filtered_df2   
# I need code change in merging the dataframes basing on the time column they have to be joined for matching records only
def mergefiltered_data(df1, df2):
    # Merge dataframes on 'date' and 'time' columns
    merged_df = pd.merge(df1, df2, on=['date', 'time'], suffixes=('_giver', '_taker'))
    return merged_df

# hey we need to send email notifications to the takers with the details of the givers for the matching records
# needto send eamil to both giver and taker
def send_email_notifications(merged_df):
    # Set up the SMTP server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(SENDER_EMAIL, SENDER_PASSWORD)
    # these are my mergedff columns
    # Timestamp_giver  Name_giver          Email ID_giver        date  ...     Name_taker          Email ID_taker Phone Number          Technical Skillset
    # hey use proper email id column names      
    for index, row in merged_df.iterrows():
        giver_email = row['Email ID_giver']
        taker_email = row['Email ID_taker']
        date = row['date']
        time = row['time']
        details = f"Giver Details:\nName: {row['Name_giver']}\nContact: {row['Phone Number']}\n\nTaker Details:\nName: {row['Name_taker']}\nContact: {row['Phone Number']}\n\nDate: {date}\nTime: {time}"
        
        subject = "Matching Record Notification"
        body = f"Dear User,\n\nThe following matching record has been found:\n\n{details}\n\nBest Regards,\nNotification System"
        message = f"Subject: {subject}\n\n{body}"
        
        # Send email to giver
        server.sendmail(SENDER_EMAIL, giver_email, message)
        # Send email to taker
        server.sendmail(SENDER_EMAIL, taker_email, message)
    
    server.quit()

if __name__ == "__main__":
    file1 = r'C:\lct-dm-p\lct-mn-auto\python_classes\mini_project\source_folders\givers.csv'
    file2 = r'C:\lct-dm-p\lct-mn-auto\python_classes\mini_project\source_folders\takers.csv'

    filtered_df1, filtered_df2 = read_and_filter_data(file1, file2)
    print("Filtered DataFrame 1:")
    print(filtered_df1)
    print("Filtered DataFrame 2:")
    print(filtered_df2)
    merged_df = mergefiltered_data(filtered_df1, filtered_df2)
    print("Merged DataFrame:")
    print(merged_df)
    send_email_notifications(merged_df)