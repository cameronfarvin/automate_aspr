# TODO
# - Pickle abstract data (variables exist, unused, in PostProcessAPSR class).

from bs4 import BeautifulSoup
from io import StringIO
from typing import Dict, List, Any
import concurrent.futures
import pandas as pd
import requests
import pickle
import time
import csv
import os

class PostProcessAPSR:
    def __init__(self, attempt_cache_load: bool=False):    
        # Semaphore for post-processing the Cambridge Core citations.
        self.cc_citations_processed = False

        # Dicts to store citation and abstract data for each title.
        self.citation_data: Dict[str, List[Any]] = {}
        self.abstract_cache = {
            'apsr_title': [],
            'citing_title': [],
            'citing_doi': [],
            'citing_abstract': [],
            'crossref_url': []
        }
        
        # Default paths for input and output files.
        self.input_csv_path: str            = "./input_data/apsr_results.csv"
        self.citations_output_csv_path: str = "./output_data/combined_apsr_citations.csv"
        self.abstract_map_csv_path: str     = "./output_data/apsr_abstract_map.csv"
        self.citations_cache_path: str      = "./cache/citation_data.pkl"
        self.abstract_cache_path: str       = "./cache/abstract_cache.pkl"
        self.credentials_path: str          = "./credentials.txt"

        # Cached file semaphors.
        self.loaded_cached_citation_data = False
        self.loaded_cached_abstract_data = False

        # Dataframe for input CSV from Cambridge Core.
        self.df: pd.DataFrame = None
        self.LoadCamCoreCSV()

        # Length of the DOI prefix (Compute once).
        self.doi_prefix_length = len("https://doi.org/")

        self.credentials = self.LoadCredentials()
        if self.credentials is None:
            print("[ Error: Could not load credentials. ]")
            return
        
        if attempt_cache_load:
            self.LoadCachedData()


    def LoadCachedData(self):
        # Load cached citation data if it exists to avoid re-fetching data
        # from web APIs if the script is run multiple times.
        if os.path.exists(self.citations_cache_path):
            try:
                with open(self.citations_cache_path, "rb") as f:
                    self.citation_data = pickle.load(f)
                    self.loaded_cached_citation_data = True
            except Exception as e:
                print(f"[ Error: Failed to load citation data: {e} ]")

        if os.path.exists(self.abstract_cache_path):
            try:
                with open(self.abstract_cache_path, "rb") as f:
                    self.abstract_cache = pickle.load(f)
                    self.loaded_cached_abstract_data = True
            except Exception as e:
                print(f"[ Error: Failed to load abstract data: {e} ]")


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
                print(f"[ Error: Failed to load credentials: {e} ]")
                return None
            return credentials


    def WebFetch(self, url: str, base_timeout: int=15, max_attempts: int = 3) -> requests.Response:
        try:
            # Retry fetching the URL with increasing timeout.
            for attempt in range(max_attempts):
                response = requests.get(url, timeout=(base_timeout * ( 2 ** attempt)))
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # If rate limited, wait for the time specified in the Retry-After header or default to 30 seconds.
                    retry_after = int(response.headers.get("Retry-After", 0))
                    if retry_after > 0: time.sleep(retry_after)
                else:
                    response.raise_for_status()
        except requests.RequestException as e:
            print(f"[ Error: Failed to fetch {url}: {e} ]")
            return None
        
    
    def LoadCamCoreCSV(self):
        try:
            print("Loading input CSV...")
            self.df            = pd.read_csv(self.input_csv_path)
            return
        except Exception as e:
            print(f"[ Error: Could not load the Cambridge Core CSV: {e} ]")
            self.df = None
            return
        
   
    def OutputCSV(self, data, output_path):
        try:
            print(f"Writing to {output_path}...")
            data.to_csv(output_path, index=False)
            return
        except Exception as e:
            print(f"[ failure ]: {e}")
            return
        

    def ProgressBar(self, iteration: int, total: int, prefix: str='', length: int=25):
        percent       = ("{0:.1f}").format(100 * (iteration / float(max(1, total))))
        filled_length = int(length * iteration // total) # '//' is floor division.
        bar           = '█' * filled_length + '-' * (length - filled_length)

        print(f'\r{prefix} |{bar}| {percent}% Complete', end='\r')
        if iteration == total:
            print()
        return

    
    def PostProcessCitations(self):
        try:
            if self.df is None:
                print("[ Error: No pre-processed citation data (JavaScript output) to post-process. ]")
                return
            
            # The input data provides a Cambridge Core URL for every paper, `P`, which returns a CSV with two columns for that paper:
            #   [ 1st Column ]: Each row contains a one-line text description (such as the author and page number) of a paper, `R`,
            #       which cites the original paper, `P`.
            #   [ 2nd Column ]: Each row contains the DOI of a paper, `R`, which cites the original paper, `P`.
            if self.loaded_cached_citation_data:
                cached_citation_data_length = len(self.citation_data)

                # If the cached data exists, only add new entries to the citation data.
                self.citation_data.update({title: [] for title in self.df["title"] if title not in self.citation_data})
                if len(self.citation_data) == cached_citation_data_length:
                    # No new entries to process.
                    print("[ Info: No new citation entries to process. Continuing... ]")
                    return
                
            else:
                # If the cached data does not exist, create a new dictionary to store citation data.
                self.citation_data = {title: [] for title in self.df["title"]}

            # Process each row in the DataFrame concurrently using ThreadPoolExecutor.
            def ProcessCitationRow(citation_row):
                paper_title                  = citation_row["title"]
                cambridge_core_citations_url = citation_row["all_citing_papers_link"]
                returned_citations           = []

                # Fetch the CSV from Cambridge Core; timeout increased to accommodate threadpool.
                cambridge_core_res = self.WebFetch(cambridge_core_citations_url)
                if cambridge_core_res is None:
                    print(f"[ Error: Skipping {cambridge_core_citations_url} ]")
                    return paper_title, []
                
                # Parse the content of the CSV returned from Cambridge Core.
                # Expected columns: [ 0: Description, 1: DOI ]
                reader = csv.reader(StringIO(cambridge_core_res.text))
                for entry in reader:
                    if len(entry) < 2:
                        print(f"[ Error: Invalid CSV entry: '{entry}' ]")
                        continue

                    # Build list of DOIs of papers, `R`, citing paper `P`.
                    doi = entry[1].strip()
                    if doi == "DOI":
                        continue  # Skip the header row.
                    elif len(doi) > self.doi_prefix_length:
                        returned_citations.append(doi)
                    else:
                        print(f"[ Error: Invalid DOI: '{doi}' ]")
                        returned_citations.append("")

                # Store the citations list in output data.
                return paper_title, returned_citations
            
            # Use ThreadPoolExecutor to process each row concurrently.
            pbar_citations_completed = 0
            pbar_citations_total = len(self.df)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(ProcessCitationRow, row) for _, row in self.df.iterrows()]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        paper_title, citations = future.result()
                        if paper_title and citations:
                            self.citation_data[paper_title].extend(citations)
                    except Exception as e:
                        print(f"[ Error: Failed to process citation row: {e} ]")
                        continue

                    pbar_citations_completed += 1
                    self.ProgressBar(pbar_citations_completed, pbar_citations_total, prefix="Processing Citations")

            # Determine longest list to standardize column lengths, and then pad each
            # list to match (for use with Pandas DataFrames).
            max_len = max(len(citations) for citations in self.citation_data.values())
            for key in self.citation_data:
                self.citation_data[key].extend([''] * (max_len - len(self.citation_data[key])))


            # Convert the citation data to a DataFrame and save it to a CSV file.
            self.OutputCSV(pd.DataFrame(self.citation_data), self.citations_output_csv_path)
            self.cc_citations_processed = True

            # Write the citation data to a cache file for later use.
            with open(self.citations_cache_path, "wb") as f:
                pickle.dump(self.citation_data, f)

            return
        except Exception as e:
            print(f"[ Error: Failed to process Cambridge Core CSV: {e} ]")
            return
        


    def PostProcessAbstracts(self):
        try:
            # Postprocess the Cambridge Core citations if not already done, as abstract
            # post-processing depends on having the extracted DOIs.
            if False == self.cc_citations_processed and False == self.loaded_cached_citation_data:                
                self.PostProcessCitations()
            
            # Fetch abstracts for each title.
            def ProcessAbstractRow(doi_row):                    
                    # Call CrossRef API to get the abstract for the DOI. Note: This route does not support
                    # the "select" parameter, so we need to fetch the entire message and extract the title and abstract.
                    crossref_doi = (doi_row[self.doi_prefix_length:]).replace(';', '').strip()
                    crossref_url = [f"https://api.crossref.org/works/{crossref_doi}"]
                    if self.credentials["email"] != "":
                        # Enable use of the "polite' version of the CrossRef API, if email is provided.
                        crossref_url.append(f"?mailto={self.credentials['email']}")
                    
                    crossref_res = self.WebFetch("".join(crossref_url))
                    if crossref_res is None:
                        print(f"[ Error: Could not fetch {crossref_url[0]} ]")
                        # Return order: title, crossref_doi, abstract, crossref_url
                        return None, crossref_doi, None, crossref_url[0]

                    # Extract and clean-up the abstract from the response
                    crossref_data = crossref_res.json()
                    abstract      = crossref_data.get("message", {}).get("abstract", "")
                    if isinstance(abstract, list):
                        abstract = " ".join(abstract)
                    abstract = BeautifulSoup(abstract, "html.parser").get_text()
                    abstract = abstract.replace("\n", " ").replace("\r", " ").strip()

                    # Extract and clean-up the title from the crossref response.
                    title = crossref_data.get("message", {}).get("title", "")
                    if isinstance(title, list):
                        title = " ".join(title)
                    title = BeautifulSoup(title, "html.parser").get_text()
                    title = title.replace("\n", " ").replace("\r", " ").strip()

                    # Return the title, doi and abstract.
                    return title, crossref_doi, abstract, crossref_url[0]
                    

            pbar_abstracts_completed = 0            
            pbar_abstracts_total     = sum(len(vals) for vals in self.citation_data.values())
            for P, R in self.citation_data.items():
                # If we're using cached data, skip the rows that have already been processed.
                if self.loaded_cached_abstract_data and (P in self.abstract_cache['apsr_title']):
                    pbar_abstracts_completed += len(R)
                    continue

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(ProcessAbstractRow, doi) for doi in R]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            self.abstract_cache['apsr_title'].append(P)
                            
                            title, doi, abstract, crossref_url = future.result()  
                            self.abstract_cache['citing_title'].append(title)
                            self.abstract_cache['citing_doi'].append(doi)
                            self.abstract_cache['citing_abstract'].append(abstract)
                            self.abstract_cache['crossref_url'].append(crossref_url)
                        except Exception as e:
                            print(f"[ Error: Failed to process abstract row: {e} ]")
                            continue

                pbar_abstracts_completed += len(R)
                self.ProgressBar(pbar_abstracts_completed, pbar_abstracts_total, prefix="Processing Abstracts")

            # Determine longest list to standardize column lengths, and then pad each
            # list to match (for use with Pandas DataFrames).
            max_len = max(len(abstracts) for abstracts in self.abstract_cache.values())
            for key in self.abstract_cache:
                self.abstract_cache[key].extend([''] * (max_len - len(self.abstract_cache[key])))

            # Convert the abstract data to a DataFrame and save it to a CSV file.
            self.OutputCSV(pd.DataFrame(self.abstract_cache), self.abstract_map_csv_path)

            # Write the abstract data to a cache file for later use.
            with open(self.abstract_cache_path, "wb") as f:
                pickle.dump(self.abstract_cache, f)
            
            return
        except Exception as e:
            print(f"[ Error: Failed to process Abstracts: {e} ]")
            return   



if __name__ == "__main__":
    try:
        # Initialize the PostProcessAPSR class.
        pproc = PostProcessAPSR(attempt_cache_load=True)

        # Fetch citations from Cambridge Core by paper title; output to CSV.
        pproc.PostProcessCitations()

        # Fetch abstracts from CrossRef by DOI; output to CSV.
        pproc.PostProcessAbstracts()

        print('[ success ]')
    except Exception as e:
        print(f'[ failure ]: {e}')