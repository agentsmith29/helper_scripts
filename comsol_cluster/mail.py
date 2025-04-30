class MailClient():

    def __init__(self, recipient: str):
        self.recipient = recipient

    def send_mail(self, text: str, subject: str = "Cluster Mail"):
        """ Send an email using the mail command """
        #print(f"Sending email to {self.recipient} with subject: {subject}: \n {text}")
        #return
        subprocess.run(
            ["mail", "-s", subject, self.recipient], 
            input=text, text=True, check=True)

