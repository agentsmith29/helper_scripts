
#! /home/myjustice/.venv/bin/python3 

import time
from typing import Iterator
import re
import subprocess
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import argparse

from mail import MailClient


class SlurmJobController():

    def __init__(self, slurm_id: str):
         self.slurm_id = slurm_id

    def _run_sacct(self, args: list[str], run_direct = True) -> dict:
        """ Run the sacct command to get job information.
        sacct - displays accounting data for all jobs and job steps in the Slurm job accounting log or Slurm database """
        if not self.slurm_id or self.slurm_id == "":
            return {"error": "Slurm ID is not set."}
            
        try:
            # Run sacct command to get start and end times
            if run_direct:
                command = ["sacct", "-j", self.slurm_id] + args
                result = subprocess.run(command, capture_output=True, text=True, check=True)
            else:
                return command
            # Parse the output
            return result
        except subprocess.CalledProcessError as e:
            return {"error": f"Failed to run sacct: {e}"}
        except Exception as e:
            return {"error": f"_run_sacct: {str(e)}"}

    def _format_sacct(self, run_direct = True, keys: str = "", *args, **kwargs):
        """ Format the output of the sacct command to get job information.
        sacct - displays accounting data for all jobs and job steps in the Slurm job accounting log or Slurm database """
        if not self.slurm_id or self.slurm_id == "":
            return {"error": "Slurm ID is not set."}


        key = f"JobID,{keys}"
        try:
            if not run_direct:
                return keys
            # Run sacct command to get start and end times
            result = self._run_sacct([f"--format={key}", "--parsable2", "--noheader"], run_direct=run_direct)

            # Parse the output
            for line in result.stdout.strip().split("\n"):
                fields = line.split("|")
                if fields[0] == self.slurm_id and fields[1] != "Unknown" :
                    if len(fields) > 1:
                        # if only two fields are returned, return the second field
                        if len(fields) == 2:
                            return fields[1]
                        # if more than two fields are returned, return the second field
                        elif len(fields) > 2:
                            return fields[1:]
            return {"error": f"Failed to run sacct with format {format}"}
        except subprocess.CalledProcessError as e:
            return {"error": f"Failed to run sacct: {e}"}
        except Exception as e:
            return {"error": f"_format_sacct: {str(e)}"}


    def runtime(self, run_direct = True, *args, **kwargs):
        _key = "elapsed" 
        return self._format_sacct(run_direct, _key)

    def state(self, run_direct = True, *args, **kwargs):
        _key = "State" 
        return self._format_sacct(run_direct, _key)
       
    def nodes(self, run_direct = True, *args, **kwargs):
        _key = "NodeList"
        return self._format_sacct(run_direct, _key)

    def name(self, run_direct = True, *args, **kwargs):
        _key = "JobName"
        return self._format_sacct(run_direct, _key)

    def alloc_cpus(self, run_direct = True, *args, **kwargs):
        _key = "AllocCPUS"
        return self._format_sacct(run_direct, _key)

    def exit_code(self, run_direct = True, *args, **kwargs):
        _key = "ExitCode"
        return self._format_sacct(run_direct, _key)
      
    def id(self, run_direct = True, *args, **kwargs):
        _key = "JobID"
        return self._format_sacct(run_direct, _key)

    def get_multiple(self, args: list[callable]):
        """ Aquire multiple jobs with the same command """
        arglist = []
        try:
            for arg in args:
                if callable(arg):
                    result = arg(run_direct=False)
                    arglist.append(result)
                else:
                    return {"error": f"Invalid argument: {arg}"}
            # Run sacct command to get start and end times
            result = self._format_sacct(run_direct=True, keys=",".join(arglist))
            print(result)

            return "..."
            # Parse the output
            for line in result.stdout.strip().split("\n"):
                fields = line.split("|")
                if fields[0] in job_ids:
                    return fields[1]

            return {"error": "Cannot find start or end time or job is still running."}

        except subprocess.CalledProcessError as e:
            return {"error": f"Failed to run sacct: {e}"}
        except Exception as e:
            return {"error": str(e)}

class FilterCommand():

    def filter_iteration(self, line):
        column_format = "{:<6} {:<6} {:<6} {:<10} {:<10} {:<10} {:<10}, {:<10}"  # Adjust widths as needed
        #rgr1 = r"^\s*[0-9.\-]+\s+[0-9.\-]+\s+[0-9.\-]+\s+\s*[0-9.\-]+\s*[0-9.\-]+\s*[0-9.\-]$"
        rgr1 = r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s+([\d.eE+-]+)\s*$"

        m = re.match(rgr1, line)
        if m:
            rgr2 = r"\s*([0-9.\-]+)"
            matches = re.findall(rgr2, m.group(0))
            if len(matches) == 6:
                _df = pd.DataFrame([matches], columns=["Iter", "Inner", "nEval", "Error", "Objective", "MaxInfeas"])
                _df['Timestamp'] = pd.Timestamp.now()
                _iter = int(_df.iloc[0]['Iter'])
                _objective = float(_df.iloc[0]['Objective'])
                mail_subject = f"Iteration finished: {_iter} ({_objective})"
                return _df, column_format, mail_subject
        return None, None, None

class ClusterLogParser():

    def __init__(self, file_name, slurm_id):
        self._file_name = Path(file_name).absolute()

        self.slurm_jobc = SlurmJobController(slurm_id)

        self._parsed_log = DataFrame()

        self._comsol_version = None
        self._loaded_file = None
        self._warnings = []
        self._num_nodes = None

    def start_watcher(self):
        # create a watcher subprocess that wakes up every 10 seconds
        # and checks if the job is still running
        # and if not, send out a mail
        watch_thread = threading.Thread(target=self._watcher_thread)
        watch_thread.daemon = True  # Daemonize thread
        watch_thread.start()  # Start the thread

    def _watcher_thread(self):
        # check if the job is still running
        # and if not, send out a mail
        _state = self.get_multiple([self.slurm_jobc.state, self.slurm_jobc.runtime])
        while True:
            time.sleep(10)
            if _state[0] != "RUNNING":
                print(f"Job {self.slurm_jobc.id()} is not running anymore")
                break

    # ==================================================================================================================
    # Extract metadata from the log file
    # ==================================================================================================================
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

    # ==================================================================================================================
    # Print header and follow the log file
    def get_metadata_str(self):
        _header = f"COMSOL Version: {self._comsol_version}"
        _header += f"\nOpened file: {self._loaded_file}"
        _header += f"\nJob Name (ID/State): {self.slurm_jobc.name()} ({self.slurm_jobc.id()}/{self.slurm_jobc.state()})"
        _header += f"\nSlurm Runtime: {self.slurm_jobc.runtime()}"
        _header += f"\nmph-File: {self._loaded_file}"
        _header += f"\nActive Workers (Processes/Assigned Nodes): {self._num_nodes} ({self.slurm_jobc.alloc_cpus()}/{self.slurm_jobc.nodes()})"
        return _header

    # ==================================================================================================================
    # Follow the log file and apply filters
    def num_lines_in_file(self):
       with open(self._file_name, "r") as f:
            return len(f.readlines())
        
    def follow(self, filters: [callable], mail_client: MailClient = None):
        printed_header = False  # Track if we've printed the header

        _num_lines = self.num_lines_in_file() 
        with open(self._file_name, 'r') as file:
            # read the numer of lines without loading the file
            line_it = 0
            print(f"File {self._file_name} has {_num_lines} lines")
            mail_subject = None
            column_format = None
            for line in self._follow_file(file):
                line_it += 1
                self.detect_comsol_version(line)
                self.detect_file_open(line)
                self.detect_nodes(line)
                
               
                for filter in filters:
                   
                    _line, _column_format, _mail_subject = filter(line)
                    if _line is not None:
                        line = _line
                    if _column_format is not None:
                        column_format = _column_format
                    if _mail_subject is not None:
                        mail_subject = _mail_subject
                                        
                    if _line is not None:
                        line['Runtime'] = self.slurm_jobc.runtime()
                        if not printed_header:
                            print(self.get_metadata_str())
                            # get the column from the dataframe
                            print(f"{column_format.format(*list(line.columns))}")
                            printed_header = True
                    
                        print(f"{column_format.format(*line.iloc[0].astype(str))}")
                        self._parsed_log = pd.concat([self._parsed_log, line], ignore_index=True)                  

                        if line_it >= _num_lines and mail_client is not None and mail_subject is not None:
                            mail_client.send_mail(
                                text=f"{self.get_metadata_str()} \n\n {self._parsed_log}",
                                subject=f"[ACluster] Job {self.slurm_jobc.id()}/{self.slurm_jobc.name()} - {mail_subject}")

                #print(f"{line_it}/{_num_lines} - MS: {mail_subject} - MC: {mail_client}")
                if line_it == _num_lines  and mail_client is not None and mail_subject is not None:
                    print(f"Sending email to {mail_client.recipient} with subject: {mail_subject}")
                    mail_client.send_mail(
                        text=f"{self.get_metadata_str()} \n\n {self._parsed_log}",
                        subject=f"[ACluster] Job {self.slurm_jobc.id()}/{self.slurm_jobc.name()} - {mail_subject}")

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

    # add a second argument for the file withput the -f
    # and --file
    parser.add_argument('file', type=str, help='Path to the COMSOL log file')
    parser.add_argument('-m', '--mail', type=str, help='Recipient email address for notifications')
    parser.add_argument('-i', '--id', type=str, help='SLURM job ID')

    args = parser.parse_args()

    fc = FilterCommand()
    # Instantiate log parser with log file
    clp = ClusterLogParser(args.file, args.id)

    # Optionally instantiate mail client
    mail_client = MailClient(args.mail) if args.mail else None

    clp.follow(filters=[fc.filter_iteration], mail_client=mail_client)

    # slurm_parser = SlurmJobController("14393295")
    # print(slurm_parser.runtime())
    # print(slurm_parser.state())
    # print(slurm_parser.nodes())
    # print(slurm_parser.name())
    # print(slurm_parser.alloc_cpus())
    # print(slurm_parser.get_multiple([slurm_parser.runtime, slurm_parser.state, slurm_parser.nodes]))
