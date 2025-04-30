import time
import threading
import subprocess
import signal
import json
from typing import Dict, Optional
from pathlib import Path
import pandas as pd 


from mail import MailClient  # Assuming you have a working mail client

class SlurmNode:
    def __init__(self, data: dict, notify_on_change: dict = None):
        self._data = data

        # Notification settings: key -> (subject_template, body_template)
        self.notify_on_change = {
            'state': (
                "Node {name} state change: {state}",  # subject
                "Node {name} changed state from {old_value} to {new_value}"  # body
            ),
            # 'last_busy': (
            #     "Node {name} last_busy change: {state}",  # subject
            #     "Node {name} changed last_busy from {old_value} to {new_value}"  # body
            # ),
            # easily add more later like 'cpu_load' etc
        }

    def __getitem__(self, item):
        return self._data.get(item)

    def __getattr__(self, item):
        try:
            return self._data[item]
        except KeyError:
            raise AttributeError(f"'SlurmNode' object has no attribute '{item}'")

    def update(self, new_data: dict):
        changes = {}
        for key, value in new_data.items():
            if self._data.get(key) != value:
                changes[key] = (self._data.get(key), value)
                self._data[key] = value
        return changes


class SlurmStatusWatcher:
    def __init__(self, 
        mail_client: Optional[MailClient] = None, 
        mail_append: Optional[callable] = None,
        notify_on_change: Optional[dict] = None):
        self.mail_client = mail_client
        self.nodes: Dict[str, SlurmNode] = {}
        self._bg_thread = None
        self._stop_event = threading.Event()
        self._start_in_background(update_interval=10)

        self.mail_append = mail_append

        signal.signal(signal.SIGINT, self._signal_handler)  # Handle Ctrl+C

    def _signal_handler(self, sig, frame):
        print("\nKeyboardInterrupt received. Shutting down cleanly...")
        self.stop()

    def _start_in_background(self, update_interval: int = 5, as_daemon: bool = False):
        print("Ramping up the watcher thread...")
        #self._bg_thread = threading.Thread(
        #    target=self.background_task,
        #    args=(update_interval,),
        #    daemon=as_daemon
        #)
        #self._bg_thread.start(
        self.background_task(update_interval=update_interval)

    def background_task(self, update_interval: int):
        print("Starting watcher thread...")
        while not self._stop_event.is_set():
            result = self._run_sinfo()
            if isinstance(result, dict):
                self._process_sinfo_json(result)
            time.sleep(update_interval)

    def _run_sinfo(self) -> Optional[dict]:
        """ Run sinfo and parse JSON """
        try:
            command = ["sinfo", "--noheader", "--json"]
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Failed to run sinfo: {e}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
        except Exception as e:
            print(f"_run_sinfo error: {e}")
        return None

    def _process_sinfo_json(self, data: dict):
        """ Parse sinfo json and update nodes """
        nodes_data = data.get("nodes", [])
        print("-", end="")
        for node_info in nodes_data:
            node_name = node_info['name']
            if node_name not in self.nodes:
                self.nodes[node_name] = SlurmNode(node_info)
            else:
                changes = self.nodes[node_name].update(node_info)
                #print(f"Node {node_name} updated: {changes}")
                # Only react to state changes (or others if needed)
                self.handle_change(self.nodes[node_name], changes)

    def handle_change(self, node: SlurmNode, changes: dict):
        """
        Generic change handler: send mail if the field is listed in node.notify_on_change
        """
        for key, (old_value, new_value) in changes.items():
            print(f"Node {node._data['name']} changed {key} from {old_value} to {new_value}")
            if key in node.notify_on_change:
                subject_template, body_template = node.notify_on_change[key]
                subject = subject_template.format(**node._data)
                body = body_template.format(**node._data, old_value=old_value, new_value=new_value)
                print(f"{subject}: {body}")
                if self.mail_append is not None:
                    print(self.mail_append())

                self.send_mail(subject=subject, text=body)

    def send_mail(self, subject: str, text: str):
        if self.mail_client:
            print(f"Sending mail: {subject}")
            self.mail_client.send_mail(subject=subject, text=text)

    def stop(self):
        """ Stop the watcher thread """
        self._stop_event.set()
        if self._bg_thread:
            self._bg_thread.join()
        print("Watcher thread stopped.")

    def __getitem__(self, node_name: str) -> SlurmNode:
        return self.nodes[node_name]


class 




if __name__ == "__main__":

    
    def custom_header(ssw_cls: 'SlurmStatusWatcher', *columns: str) -> str:
        """ 
        Construct a table from current nodes using Pandas.
        
        Args:
            ssw_cls (SlurmStatusWatcher): the watcher instance
            columns (str): the columns to include
        
        Returns:
            str: a nicely formatted table (as text)
        """
        # Gather all node data into a list of dicts
        data = [node._data for node in ssw_cls.nodes.values()]
        
        if not data:
            return "No nodes available."

        df = pd.DataFrame(data)
        
        # Only keep the requested columns if they exist
        available_columns = [col for col in columns if col in df.columns]
        if not available_columns:
            return "Requested columns not available."

        df = df[available_columns]

        # Format nicely as plain text table
        return df.to_string(index=False)


    ssw = SlurmStatusWatcher(mail_append=lambda s: custom_header(s, "partition", "avail", "timelimit", "nodes", "state", "nodelist"))
   
    time.sleep(5)  # Allow some time for the watcher to gather data
    # node1 = ssw["node1"]
    #print(node1.alloc_cpus)
    #print(node1.state)