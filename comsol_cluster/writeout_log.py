
import time
from typing import Iterator
import re
import subprocess
import pandas as pd
from pandas import DataFrame

class MailClient():

    def __init__(self, recipient: str):
        self.recipient = recipient

    def send_mail(self, text: str, subject: str = "Cluster Mail"):
        """ Send an email using the mail command """
        subprocess.run(
            ["mail", "-s", subject, self.recipient], 
            input=text, text=True, check=True)


class DefaultRegex():

    def filter_iteration(self, line):
        rgr1 = r"^\s*[0-9.\-]+\s+[0-9.\-]+\s+[0-9.\-]+\s+\s*[0-9.\-]+\s*[0-9.\-]+\s*[0-9.\-]$"
        m = re.match(rgr1, line)
        if m:
            rgr2 = r"\s*([0-9.\-]+)"
            matches = re.findall(rgr2, m.group(0))
            _df = pd.DataFrame([matches], columns=["Iter", "Inner", "nEval", "Error", "Objective", "MaxInfeas"])
            return _df
        return None


class ClusterLogParser():

    def __init__(self, file_name):
        self._file_name = file_name
        self.default_regex = DefaultRegex()
        self._parsed_log = DataFrame()


    def follow(self, filter: callable, mail_client: MailClient = None):
        printed_header = False  # Track if we've printed the header
        column_format = "{:<6} {:<6} {:<6} {:<10} {:<10} {:<10} {:<10}"  # Adjust widths as needed

        with open(self._file_name, 'r') as file:
            for line in self._follow_file(file):
                line = filter(line)
                if line is not None:
                    line['Timestamp'] = pd.Timestamp.now()
                    if not printed_header:
                        print(column_format.format("Iter", "Inner", "nEval", "Error", "Objective", "MaxInfeas", 'Timestamp'))
                        printed_header = True
                   
                    print(column_format.format(*line.iloc[0].astype(str)))
                    self._parsed_log = pd.concat([self._parsed_log, line], ignore_index=True)                  

                    
                    if mail_client is not None:
                        mail_client.send_mail(
                            text=f"{self._parsed_log}",
                            subject=f"New iteration finished: {self._parsed_log.iloc[-1]['Iter']}")




    def _follow_file(self, file, sleep_sec=0.1) -> Iterator[str]:
        """ Yield each line from a file as they are written.
        `sleep_sec` is the time to sleep after empty reads. """
        line = ''
        while True:
            tmp = file.readline()
            if tmp is not None and tmp != "":
                line += tmp
                if line.endswith("\n"):
                    yield line
                    line = ''
            elif sleep_sec:
                # capture Ctrl-C
                # and other signals
                # to break the loop
                # and exit gracefully
                try:
                    time.sleep(sleep_sec)
                except KeyboardInterrupt:
                    print("Closing Log")
                    break

    def _apply_regex(self, expression, line):
        """ Apply regex to the line and return the result """
        line = line.strip()
        # Apply the regex to the line
        # and return the result
        if not expression:
            return line
        m = re.findall(expression, line)
        if m is None:
            return ""
        return m

    




if __name__ == '__main__':
    clp = ClusterLogParser("teslog.log")
    mail = MailClient("christoph.schmidt@tugraz.at")
    clp.follow(filter=clp.default_regex.filter_iteration, mail_client=mail)
    # clp.follow(filter=None)

    