import threading
from bs4 import BeautifulSoup
from io import StringIO
from typing import Dict, List, Any
import concurrent.futures
import pandas as pd
import requests
import inspect
import pickle
import time
import csv
import os


class PostProcessAPSR:
    def __init__(self, attempt_cache_load: bool = False):
        # Semaphore for post-processing the Cambridge Core citations.
        self.cc_citations_processed = False

        # Citation data: key = apsr_title, value = list of DOIs.
        self.citation_data: Dict[str, List[Any]] = {}

        # Each value is a dictionary with keys: apsr_title, citing_doi, citing_title, citing_abstract, crossref_url.
        self.abstract_cache: Dict[str, Dict[str, Any]] = {}

        # Default paths for input and output files.
        self.input_csv_path: str = "./input_data/apsr_results.csv"
        self.citations_output_csv_path: str = "./output_data/combined_apsr_citations.csv"
        self.abstract_map_csv_path: str = "./output_data/apsr_abstract_map.csv"
        self.citations_cache_path: str = "./cache/citation_data.pkl"
        self.abstract_cache_path: str = "./cache/abstract_cache.pkl"
        self.credentials_path: str = "./credentials.txt"
        self.log_path: str = "./log.txt"

        # Cache flags.
        self.loaded_cached_citation_data = False
        self.loaded_cached_abstract_data = False

        # DataFrame for input CSV from Cambridge Core.
        self.df: pd.DataFrame = None
        self.LoadCamCoreCSV()

        # Compute DOI prefix length.
        self.doi_prefix_length: int = len("https://doi.org/")

        self.credentials = self.LoadCredentials()

        # Create a persistent session for HTTP requests.
        self.session = requests.Session()

        # Lock for thread-safe updates to abstract_cache.
        self.abstract_lock = threading.Lock()

        if attempt_cache_load:
            self.LoadCachedData()

        # Initialize the log with a message of type "info" indicating the start date/time of the process.
        self.log_helper = lambda: (inspect.currentframe().f_back.f_code.co_name,
                                   inspect.currentframe().f_back.f_lineno)
        self.log = {
            'info': [f"Post-processing started at {time.strftime('%Y-%m-%d %H:%M:%S')}"]}

    def Log(self, type: str, fn_name: str, line_no: int, message: str):
        # Log messages with function name and line number.
        if type not in self.log:
            self.log[type] = []
        if message:
            self.log[type].append(
                f"[ {type} ]: {fn_name} (line {line_no}): {message}")

    def LoadCachedData(self):
        # Load cached citation data.
        if os.path.exists(self.citations_cache_path):
            try:
                with open(self.citations_cache_path, "rb") as f:
                    self.citation_data = pickle.load(f)
                    self.loaded_cached_citation_data = True
            except Exception as e:
                fn, line = self.log_helper()
                self.Log("error", fn, line,
                         f"Failed to load citation data: {e}")
                raise e

        # Load cached abstract data.
        if os.path.exists(self.abstract_cache_path):
            try:
                with open(self.abstract_cache_path, "rb") as f:
                    self.abstract_cache = pickle.load(f)
                    self.loaded_cached_abstract_data = True
            except Exception as e:
                fn, line = self.log_helper()
                self.Log("error", fn, line,
                         f"Failed to load abstract data: {e}")
                raise e

    def LoadCredentials(self):
        if os.path.exists(self.credentials_path):
            credentials = {}
            try:
                with open(self.credentials_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        key, value = line.split(":", 1)
                        credentials[key.strip()] = value.strip()
            except Exception as e:
                fn, line = self.log_helper()
                self.Log("error", fn, line, f"Failed to load credentials: {e}")
                return None
            return credentials

    def WebFetch(self, url: str, base_timeout: int = 15, max_attempts: int = 5) -> requests.Response:
        wait = base_timeout
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, timeout=wait)
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # Use safe conversion for the Retry-After header (capped at 180 sec).
                    retry_after_header = response.headers.get("Retry-After")
                    if retry_after_header is not None:
                        try:
                            retry_after = int(retry_after_header)
                        except ValueError:
                            retry_after = wait
                    else:
                        retry_after = wait
                    time.sleep(min(retry_after, 180))
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                fn, line = self.log_helper()
                self.Log("error", fn, line, f"Webfetch attempt {attempt+1} failed for {url}: \
                         Response: {response}; Exception: {e}")
            wait = min(wait * 2, 180)
            time.sleep(wait)

        fn, line = self.log_helper()
        self.Log("error", fn, line,
                 f"All {max_attempts} attempts failed for {url}")
        return None

    def LoadCamCoreCSV(self):
        try:
            print("Loading input CSV...")
            self.df = pd.read_csv(self.input_csv_path)
        except Exception as e:
            fn, line = self.log_helper()
            self.Log("error", fn, line,
                     f"Failed to load Cambridge Core CSV: {e}")
            self.df = None
            raise e

    def OutputCSV(self, data, output_path):
        try:
            print(f"Writing to {output_path}...")
            data.to_csv(output_path, index=False)
        except Exception as e:
            fn, line = self.log_helper()
            self.Log("error", fn, line, f"Failed to write CSV: {e}")

    def ProgressBar(self, iteration: int, total: int, prefix: str = '', length: int = 25):
        percent = "{0:.1f}".format(100 * (iteration / float(max(1, total))))
        filled_length = int(length * iteration // total)
        bar = '█' * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% Complete', end='\r')
        if iteration == total:
            print()

    def PostProcessCitations(self):
        try:
            if self.df is None:
                fn, line = self.log_helper()
                self.Log("error", fn, line,
                         "No input data loaded. Cannot process citations.")
                return

            # The input data provides a Cambridge Core URL for every paper, `P`, which returns a CSV with two columns for that paper:
            #   [ 1st Column ]: Each row contains a one-line text description (such as the author and page number) of a paper, `R`,
            #       which cites the original paper, `P`.
            #   [ 2nd Column ]: Each row contains the DOI of a paper, `R`, which cites the original paper, `P`.
            if self.loaded_cached_citation_data:
                cached_length = len(self.citation_data)
                self.citation_data.update(
                    {title: [] for title in self.df["title"] if title not in self.citation_data})
                if len(self.citation_data) == cached_length:
                    fn, line = self.log_helper()
                    self.Log("info", fn, line,
                             "No new citation data to process; continuing.")
                    return
            else:
                self.citation_data = {title: [] for title in self.df["title"]}

            def ProcessCitationRow(citation_row):
                paper_title = citation_row["title"]
                cambridge_core_citations_url = citation_row["all_citing_papers_link"]
                returned_citations = []
                res = self.WebFetch(cambridge_core_citations_url)
                if res is None:
                    fn, line = self.log_helper()
                    self.Log(
                        "error", fn, line, f"Failed to fetch {cambridge_core_citations_url}; skipping.")
                    return paper_title, []
                reader = csv.reader(StringIO(res.text))
                for entry in reader:
                    if len(entry) < 2:
                        fn, line = self.log_helper()
                        self.Log(
                            "error", fn, line, f"Invalid CSV entry: {entry} for {paper_title}; skipping.")
                        continue
                    doi = entry[1].strip()
                    if doi == "DOI":
                        continue
                    elif len(doi) > self.doi_prefix_length:
                        returned_citations.append(doi)
                    else:
                        fn, line = self.log_helper()
                        self.Log(
                            "error", fn, line, f"Invalid DOI: '{doi}' for {paper_title}; skipping.")
                        returned_citations.append("")
                return paper_title, returned_citations

            pbar_completed = 0
            total_papers = len(self.df)
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(ProcessCitationRow, row)
                           for _, row in self.df.iterrows()]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        paper_title, citations = future.result()
                        if paper_title and citations:
                            self.citation_data.setdefault(
                                paper_title, []).extend(citations)
                    except Exception as e:
                        fn, line = self.log_helper()
                        self.Log("error", fn, line,
                                 f"Failed to process citation row: {e}")
                    pbar_completed += 1
                    self.ProgressBar(pbar_completed, total_papers,
                                     prefix="Processing Citations")

            max_len = max(len(cites) for cites in self.citation_data.values())
            for key in self.citation_data:
                self.citation_data[key].extend(
                    [''] * (max_len - len(self.citation_data[key])))
            self.OutputCSV(pd.DataFrame(self.citation_data),
                           self.citations_output_csv_path)
            self.cc_citations_processed = True
            with open(self.citations_cache_path, "wb") as f:
                pickle.dump(self.citation_data, f)
        except Exception as e:
            fn, line = self.log_helper()
            self.Log("error", fn, line,
                     f"Failed to process Cambridge Core citations: {e}")
            raise e

    def PostProcessAbstracts(self):
        try:
            if not self.cc_citations_processed and not self.loaded_cached_citation_data:
                self.PostProcessCitations()

            # Build list of (apsr_title, doi) pairs that need processing.
            to_process = []
            for apsr_title, doi_list in self.citation_data.items():
                for doi in doi_list:
                    # Skip blank DOIs.
                    if not doi.strip():
                        continue
                    record = self.abstract_cache.get(doi, {})
                    if (not record.get('citing_title') or not record.get('citing_abstract') or
                        not record.get('apsr_title') or not record.get('citing_doi') or
                            not record.get('crossref_url')):
                        to_process.append((apsr_title, doi))

            total_to_process = len(to_process)
            print(f"Total abstract entries to process: {total_to_process}")
            processed_count = 0

            def ProcessAbstractRow(pair):
                apsr_title, doi = pair
                # Handle DOI prefix if not present.
                if doi.startswith("https://doi.org/"):
                    crossref_doi = (doi[self.doi_prefix_length:]).replace(
                        ';', '').strip()
                else:
                    crossref_doi = doi.replace(';', '').strip()

                url = f"https://api.crossref.org/works/{crossref_doi}"
                if self.credentials.get("email", ""):
                    url += f"?mailto={self.credentials['email']}"
                
                res = self.WebFetch(url)
                if res is None:
                    fn, line = self.log_helper()
                    self.Log("error", fn, line,
                             f"Failed to fetch {url}; continuing.")
                    return (apsr_title, doi, "", "", url)
                data = res.json().get("message", {})
                

                title_val = data.get("title", "")
                if isinstance(title_val, list):
                    title_val = " ".join(title_val)
                title_val = BeautifulSoup(
                    title_val, "html.parser").get_text().strip()
                
                # Extract and clean citing abstract.
                abstract_val = data.get("abstract", "")
                if isinstance(abstract_val, list):
                    abstract_val = " ".join(abstract_val)
                abstract_val = BeautifulSoup(abstract_val, \
                                             "html.parser").get_text().strip()
                return (apsr_title, doi, title_val, abstract_val, url)

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(
                    ProcessAbstractRow, pair): pair for pair in to_process}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        apsr_title, doi, citing_title, citing_abstract, crossref_url = future.result()
                        with self.abstract_lock:
                            self.abstract_cache[doi] = {
                                'apsr_title': apsr_title,
                                'citing_doi': doi,
                                'citing_title': citing_title,
                                'citing_abstract': citing_abstract,
                                'crossref_url': crossref_url
                            }
                    except Exception as e:
                        fn, line = self.log_helper()
                        self.Log("error", fn, line,
                                 f"Failed to process abstract row: {e}")
                        continue

                    processed_count += 1
                    # Update the progress bar on every processed abstract.
                    self.ProgressBar(
                        processed_count, total_to_process, prefix="Processing Abstracts")

                    # (Batch update) Every 100 processed abstracts, flush CSV and pickle out.
                    if processed_count % 100 == 0:
                        self.OutputCSV(pd.DataFrame.from_dict(self.abstract_cache, orient='index'),
                                       self.abstract_map_csv_path)
                        with open(self.abstract_cache_path, "wb") as f:
                            pickle.dump(self.abstract_cache, f)
                        # Force an immediate progress update after flushing.
                        self.ProgressBar(
                            processed_count, total_to_process, prefix="Processing Abstracts")

            # Final CSV and pickle update after processing is complete.
            self.OutputCSV(pd.DataFrame.from_dict(self.abstract_cache, orient='index'),
                           self.abstract_map_csv_path)
            with open(self.abstract_cache_path, "wb") as f:
                pickle.dump(self.abstract_cache, f)
        except Exception as e:
            fn, line = self.log_helper()
            self.Log("error", fn, line, f"Failed to process abstracts: {e}")
            raise e


if __name__ == "__main__":
    try:
        pproc = PostProcessAPSR(attempt_cache_load=True)
        pproc.PostProcessCitations()
        pproc.PostProcessAbstracts()
    except Exception as e:
        print(f'[ failure ]: {e}')
    finally:
        pproc.session.close()
        total_logs = sum(len(messages) for messages in pproc.log.values())
        if total_logs > 0:
            print(
                f"Note: There are {total_logs} log messages; check {pproc.log_path} for details.")
            with open(pproc.log_path, "w") as log_file:
                for log_type, messages in pproc.log.items():
                    for message in messages:
                        log_file.write(f"{message}\n")
        else:
            print("[ no log messages generated ]")
