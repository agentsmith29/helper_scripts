
#! /home/mayjustice/.venv/bin/python3 

import time
from typing import Iterator
import re
import subprocess
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import argparse

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
        #rgr1 = r"^\s*[0-9.\-]+\s+[0-9.\-]+\s+[0-9.\-]+\s+\s*[0-9.\-]+\s*[0-9.\-]+\s*[0-9.\-]$"
        rgr1 = r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s*$"

        m = re.match(rgr1, line)
        if m:
            rgr2 = r"\s*([0-9.\-]+)"
            matches = re.findall(rgr2, m.group(0))
            if len(matches) == 6:
                _df = pd.DataFrame([matches], columns=["Iter", "Inner", "nEval", "Error", "Objective", "MaxInfeas"])
                # try to convert ever
                # _df.apply(pd.to_numeric)

                _df['Timestamp'] = pd.Timestamp.now()
                return _df
        return None

class ClusterLogParser():

    def __init__(self, file_name, slurm_id):
        self._file_name = Path(file_name).absolute()
       
        self.slurm_id = slurm_id

        self.default_regex = DefaultRegex()
        self._parsed_log = DataFrame()

        self._comsol_version = None
        self._loaded_file = None
        self._warnings = []
        self._num_nodes = None

    def get_slurm_id(self):
        with open(self.slurm_status_file, 'r') as file:
            return file.readline()[0].strip()
    
    def get_slurm_runtime(self, job_id: str):
        try:
            # Run sacct command to get start and end times
            result = subprocess.run(
                ["sacct", "-j", job_id, "--format=JobID,elapsed", "--parsable2", "--noheader"],
                capture_output=True,
                text=True,
                check=True
            )

            # Parse the output
            for line in result.stdout.strip().split("\n"):
                fields = line.split("|")
                if fields[0] == job_id and fields[1] != "Unknown" :
                    return fields[1]

            return {"error": "Start or end time not found or job is still running."}

        except subprocess.CalledProcessError as e:
            return {"error": f"Failed to run sacct: {e}"}
        except Exception as e:
            return {"error": str(e)}


    def detect_comsol_version(self, line: str):
        """Detect and store COMSOL version from header line"""
        match = re.search(r"\*\*\*COMSOL\s+([^\s]+)", line)
        if match:
            self._comsol_version = match.group(1)

    def detect_nodes(self, line: str):
        """Detect and store number of nodes used"""
        match = re.search(r"distributed mode using (\d+) nodes", line)
        if match:
            self._num_nodes = int(match.group(1))

    def detect_file_open(self, line: str):
        """Detect and store .mph file being opened"""
        match = re.search(r"Opening file:\s+(.*\.mph)", line)
        if match:
            self._loaded_file = match.group(1)

    def get_metadata(self):
        return {
            "version": self._comsol_version,
            "file": self._loaded_file,
            "warnings": self._warnings,
            "nodes": self._num_nodes,
        }
    
    def print_header(self):
        _header = f"COMSOL Version: {self._comsol_version}"
        _header += f"\nOpened file: {self._loaded_file}"
        _header += f"\nSlurm ID: {self.slurm_id}"
        _header += f"\nmph-File: {self._loaded_file}"
        _header += f"\nActive Workers: {self._num_nodes}"
        return _header

    def follow(self, filters: [callable], mail_client: MailClient = None):
        printed_header = False  # Track if we've printed the header
        column_format = "{:<6} {:<6} {:<6} {:<10} {:<10} {:<10} {:<10}, {:<10}"  # Adjust widths as needed

        with open(self._file_name, 'r') as file:


            for line in self._follow_file(file):
                self.detect_comsol_version(line)
                self.detect_file_open(line)
                self.detect_nodes(line)

                for filter in filters:
                    line = filter(line)
                   
                    if line is not None:
                        line['Runtime'] = self.get_slurm_runtime(self.slurm_id)
                        if not printed_header:
                            print(self.print_header())
                            print(column_format.format("Iter", "Inner", "nEval", "Error", "Objective", "MaxInfeas", 'Timestamp', 'Runtime'))
                            printed_header = True
                    
                        print(column_format.format(*line.iloc[0].astype(str)))
                        self._parsed_log = pd.concat([self._parsed_log, line], ignore_index=True)                  

                    
                        if mail_client is not None:
                            mail_client.send_mail(
                                text=f"{self.print_header()} \n\n {self._parsed_log}",
                                subject=f"ID {self.slurm_id} - Iteration finished: {self._parsed_log.iloc[-1]['Iter']}")

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
    parser = argparse.ArgumentParser(description="Parse COMSOL cluster logs and optionally notify via email.")

    parser.add_argument('-f', '--file', type=str, required=True, help='Path to the COMSOL log file')
    parser.add_argument('-m', '--mail', type=str, help='Recipient email address for notifications')
    parser.add_argument('-i', '--id', type=str, help='SLURM job ID')

    args = parser.parse_args()

    # Instantiate log parser with log file
    clp = ClusterLogParser(args.file, args.id)

    # Optionally instantiate mail client
    mail_client = MailClient(args.mail) if args.mail else None
    clp.follow(filters=[clp.default_regex.filter_iteration], mail_client=mail_client)

    