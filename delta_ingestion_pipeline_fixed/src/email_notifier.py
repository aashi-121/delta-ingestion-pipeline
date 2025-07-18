import yagmail

def send_email_notification(dataframe):
    receiver = "receiver_email@example.com"
    subject = "Delta Table Update Notification"
    html_body = dataframe.toPandas().to_html(index=False)
    yag = yagmail.SMTP("your_email@example.com", "your_app_password")
    yag.send(to=receiver, subject=subject, contents=html_body)