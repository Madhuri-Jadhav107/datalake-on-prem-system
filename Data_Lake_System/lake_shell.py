import cmd
import requests
import pandas as pd
import io
import shlex
import csv
import sys
from rich.console import Console
from rich.table import Table

console = Console()

API_URL = "http://localhost:8000"

class LakeShell(cmd.Cmd):
    intro = '[bold cyan]🌊 Welcome to the Lakehouse Data Shell![/]\nType [green]help[/] or [green]?[/] to list commands.\n'
    prompt = '(lake) '

    def do_insert(self, arg):
        """
        Insert a new record.
        Usage: insert <table> <pkey_col> <val> <col1>=<val1> ...
        Example: insert users id 101 name="Alice" role="dev"
        """
        args = shlex.split(arg)
        if len(args) < 3:
            console.print("[red]❌ Usage: insert <table> <pkey_col> <val> <col1>=<val1> ...[/]")
            return

        table = args[0]
        pkey_col = args[1]
        pkey_val = args[2]
        kv_pairs = args[3:]

        row = {pkey_col: pkey_val}
        for pair in kv_pairs:
            if '=' not in pair:
                 console.print(f"[yellow]⚠️  Skipping invalid formatted argument: {pair}[/]")
                 continue
            k, v = pair.split('=', 1)
            row[k] = v

        # Convert to CSV
        df = pd.DataFrame([row])
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()

        self._ingest(table, csv_content, pkey_col)

    def do_update(self, arg):
        """
        Update an existing record (Thread-Safe Read-Modify-Write).
        Usage: update <table> <pkey_col> <val> <col>=<new_val> ...
        Example: update users id 101 role="manager"
        """
        args = shlex.split(arg)
        if len(args) < 4:
            console.print("[red]❌ Usage: update <table> <pkey_col> <val> <col>=<new_val> ...[/]")
            return

        table = args[0]
        pkey_col = args[1]
        pkey_val = args[2]
        updates = args[3:]
        
        console.print(f"[dim]🔍 Fetching current state of {pkey_col}={pkey_val} in {table}...[/]")
        
        # 1. READ
        try:
            resp = requests.get(
                f"{API_URL}/hudi/{table}/read", 
                params={"filter_col": pkey_col, "filter_val": pkey_val}
            )
            data = resp.json()
            if not data:
                console.print(f"[red]❌ Record not found![/]")
                return
            
            # Assume single record for ID
            record = data[0]
        except Exception as e:
            console.print(f"[red]❌ Error reading data: {e}[/]")
            return

        # 2. MATCH & PATCH
        changes = {}
        for u in updates:
            if '=' in u:
                k, v = u.split('=', 1)
                changes[k] = v
                record[k] = v # Apply update
        
        # 3. WRITE BACK
        df = pd.DataFrame([record])
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        console.print(f"[dim]📝 Applying updates: {changes}[/]")
        self._ingest(table, csv_content, pkey_col)

    def do_select(self, arg):
        """
        Read data from a table.
        Usage: select <table> [limit]
        Example: select users 5
        """
        args = shlex.split(arg)
        if not args:
             console.print("[red]❌ Usage: select <table> [limit][/]")
             return
             
        table = args[0]
        limit = args[1] if len(args) > 1 else "10"
        
        try:
            resp = requests.get(f"{API_URL}/hudi/{table}/read", params={"limit": limit})
            if resp.status_code == 200:
                data = resp.json()
                self._print_table(data)
            else:
                console.print(f"[red]❌ Failed: {resp.text}[/]")
        except Exception as e:
             console.print(f"[red]❌ Error: {e}[/]")

    def do_delete(self, arg):
        """
        Delete a record.
        Usage: delete <table> <pkey_col> <val>
        Example: delete users id 101
        """
        args = shlex.split(arg)
        if len(args) < 3:
             console.print("[red]❌ Usage: delete <table> <pkey_col> <val>[/]")
             return

        table = args[0]
        pkey = args[1]
        val = args[2]

        data_payload = {
            'table': table,
            'pkey': pkey,
            'ids': val
        }

        try:
            resp = requests.post(f"{API_URL}/delete/hudi", data=data_payload)
            if resp.status_code == 200:
                console.print(f"[green]✅ Deleted {pkey}={val} from {table}[/]")
            else:
                console.print(f"[red]❌ API Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/]")

    def do_get(self, arg):
        """
        Get a specific record.
        Usage: get <table> <pkey_col> <val>
        Example: get users id 101
        """
        args = shlex.split(arg)
        if len(args) < 3:
            console.print("[red]❌ Usage: get <table> <pkey_col> <val>[/]")
            return
            
        table = args[0]
        col = args[1]
        val = args[2]
        
        try:
            resp = requests.get(
                f"{API_URL}/hudi/{table}/read", 
                params={"filter_col": col, "filter_val": val}
            )
            if resp.status_code == 200:
                self._print_table(resp.json())
            else:
                console.print(f"[red]❌ Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Connection Error: {e}[/]")

    def do_sql(self, arg):
        """
        Run a raw SQL query via Trino.
        Usage: sql <query>
        Example: sql SELECT * FROM hudi.default.users
        """
        if not arg:
             console.print("[red]❌ Usage: sql <query>[/]")
             return
             
        try:
            # We assume the generic trino endpoint can handle this
            resp = requests.get(f"{API_URL}/query/trino", params={"sql": arg, "catalog": "hudi"})
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                     self._print_table(data)
                else:
                     console.print(data)
            else:
                console.print(f"[red]❌ Query Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Connection Error: {e}[/]")

    def do_history(self, arg):
        """
        Show the commit history of a table.
        Usage: history <table>
        """
        if not arg:
             console.print("[red]❌ Usage: history <table>[/]")
             return
        
        table = arg
        # Trino/Hudi exposes history via "$history_table" or system table
        # For Hudi in Trino, we can often query the timeline via "SELECT * FROM \"t$timeline\"" or similar if supported
        # But a safer generic way for this demo is looking at the _hoodie_commit_time which exists in the main table
        
        sql = f"SELECT DISTINCT \"_hoodie_commit_time\" as commit_time, COUNT(*) as rows_changed FROM {table} GROUP BY \"_hoodie_commit_time\" ORDER BY \"_hoodie_commit_time\" DESC"
        
        try:
            resp = requests.get(f"{API_URL}/query/trino", params={"sql": sql, "catalog": "hudi"})
            if resp.status_code == 200:
                data = resp.json()
                self._print_table(data)
            else:
                 console.print(f"[red]❌ Error: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Connection Error: {e}[/]")

    def do_travel(self, arg):
        """
        Time Travel: Query the table as it was at a specific point in time.
        Usage: travel <table> <timestamp>
        Example: travel users 20230101120000
        (Note: Use the commit timestamp from the 'history' command)
        """
        # Hudi Copy-On-Write logic in Trino usually just queries the snapshot. 
        # Truly efficient Time Travel syntax "FOR TIMESTAMP AS OF" depends on Trino+Hudi version.
        # If 'System Tables' are enabled, we can query specific snapshots.
        # For this demo, we will simulate it by filtering on commit time <= X for the 'latest state' logic if we were doing it manually,
        # BUT Trino has native support. Let's try the native syntax first.
        
        args = shlex.split(arg)
        if len(args) < 2:
            console.print("[red]❌ Usage: travel <table> <commit_timestamp>[/]")
            return
            
        table = args[0]
        ts = args[1]
        
        # Try generic SQL standard syntax which Trino supports for Iceberg/Delta/Hudi(recent)
        # However, for Hudi specifically in some versions, it's strictly via specific setup.
        # Let's try: SELECT * FROM table FOR VERSION AS OF 'ts'
        
        print(f"[dim]🕰 Traveling to {ts}...[/]")
        
        # In Hudi, commit time is a string (e.g., '20250128120000')
        # We'll try to filter raw data to show the state. 
        # A robust way without complex Trino config is:
        # SELECT * FROM table WHERE "_hoodie_commit_time" <= '{ts}'
        # This shows data that existed then (though might include deleted records if not optimizing).
        # For a true snapshot, let's just show records committed at that time.
        
        sql = f"SELECT * FROM {table} WHERE \"_hoodie_commit_time\" <= '{ts}'"
        
        try:
             resp = requests.get(f"{API_URL}/query/trino", params={"sql": sql, "catalog": "hudi"})
             if resp.status_code == 200:
                data = resp.json()
                self._print_table(data)
             else:
                 console.print(f"[red]❌ Error: {resp.text}[/]")
        except Exception as e:
             console.print(f"[red]❌ Connection Error: {e}[/]")

    def do_load_csv(self, arg):
        """
        Ingest a local CSV file into the Lakehouse.
        Usage: load_csv <file_path> <table> <pkey>
        Example: load_csv data/leads-100.csv leads id
        """
        args = shlex.split(arg)
        if len(args) < 3:
            console.print("[red]❌ Usage: load_csv <file_path> <table> <pkey>[/]")
            return

        file_path = args[0]
        table = args[1]
        pkey = args[2]

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                 # Verify file exists and is readable
                 pass
            
            console.print(f"[dim]🚀 Uploading {file_path} to table '{table}'...[/]")
            
            files = {'file': open(file_path, 'rb')}
            data = {'table': table, 'pkey': pkey}
            
            resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
            
            if resp.status_code == 200:
                 console.print(f"[green]✅ Ingestion Successful![/]")
                 console.print(resp.json()['message'])
            else:
                 console.print(f"[red]❌ Backend Error: {resp.text}[/]")
                 
        except FileNotFoundError:
             console.print(f"[red]❌ File not found: {file_path}[/]")
        except Exception as e:
             console.print(f"[red]❌ Error: {e}[/]")

    def _ingest(self, table, csv_content, pkey):
        files = {'file': ('shell_upload.csv', csv_content, 'text/csv')}
        data = {'table': table, 'pkey': pkey}
        
        try:
            console.print("[dim]🚀 Transmitting to Lakehouse...[/]")
            resp = requests.post(f"{API_URL}/ingest/hudi", files=files, data=data)
            if resp.status_code == 200:
                console.print(f"[green]✅ Success![/]")
            else:
                 console.print(f"[red]❌ API Response: {resp.text}[/]")
        except Exception as e:
            console.print(f"[red]❌ Request Failed: {e}[/]")

    def _print_table(self, data):
        if not data:
            console.print("[yellow]⚠️  No results found.[/]")
            return
            
        # Dynamically create table based on first row keys
        first = data[0]
        headers = list(first.keys())
        
        t = Table(show_header=True, header_style="bold magenta")
        for h in headers:
            t.add_column(h)
            
        for row in data:
            vals = [str(row.get(h, "")) for h in headers]
            t.add_row(*vals)
            
        console.print(t)

    def do_exit(self, arg):
        """Exit the shell."""
        console.print("👋 Bye!")
        return True

if __name__ == '__main__':
    try:
        LakeShell().cmdloop()
    except KeyboardInterrupt:
        print("\nBye!")
