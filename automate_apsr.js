/*
 * [ File ]:
 * automate_apsr.js
 * 
 * [ Description ]: Expected Initial Conditions
 * This script automates the collection of certain data from the APSR (Cambridge Core) Web Search interface.
 * It is designed to be run in the browser's console window (more on this momentarily) with the first page of the
 * desired search results visible in the browser. For example, the study which prompted the creation of this script
 * used the following results page as the initial conditions for running this script: http://tiny.cc/8dbf001.
 * 
 * [ Description ]: Work Performed
 * This script will retrieve information for the given quantity of search results defined in the variable
 * "SEARCH_RESULT_COLLECTION_COUNT" at the top of the file. The script retrieves information such as the title,
 * authors, (online) publication date, citation count, abstract, and links to all citing papers for the specified
 * collection count. The collected data is then saved as a CSV file. Offline post-processing of the data is available
 * in other (Python) scripts included in this repository.
 * 
 * [ Note ]: Running JavaScript in the Browser Console.
 * If you are unfamiliar with running JavasScript from the console of a browser, it is easier than it sounds.
 * I recommend chatting with your preferred LLM to learn how to do so. There are also tutorials and videos for
 * this online. Once you have access to the browser's console, all that is required is to (a) set the initial
 * conditions for the search results in the apsr Cambridge Core web interface (conduct your search), then (b) set the
 * "SEARCH_RESULT_COLLECTION_COUNT" variable in this file to the desired number of search results to collect, then (c)
 * copy and paste this script into the console, and (d) press enter. ***Given that there have been no substantive changes
 * to the apsr Cambridge Core web interface since the time of this writing, this script should automatically collect
 * the desired data.***
 */

const SEARCH_RESULT_COLLECTION_COUNT = 100; // The number of results to collect. Adjust as needed.

/**
 * Function to automate the collection of data from the APSR web interface.
 * @param {number} collection_count - The number of results to collect.
 */
async function automate_apsr(collection_count) {
    const apsr_data = [];

    const base_load_buffer_ms = 3000; // Base wait time in milliseconds for loading elements.
    let next_page_link;
    let page_count = 0;

    const errors = [];
    const warnings = [];

    /**
     * Function to collect data from multiple pages until the desired collection count is reached.
     */
    async function collect_apsr_data() {
        while (apsr_data.length < collection_count) {
            await collect_apsr_page_data();

            // After collecting data from the current page, check if we have enough data.
            if (apsr_data.length >= collection_count) {
                break;
            }

            if (!next_page_link) {
                errors.push('Next page link not found on page ' + page_count + '.');
                break;
            }
            else {
                next_page_link.click();
                await new Promise(resolve => setTimeout(resolve, 2 * base_load_buffer_ms));
                page_count++;
            }
        }

        console.log(JSON.stringify(apsr_data, null, 3));
        DownlaodResultsAsCSV(apsr_data, 'apsr_results.csv');
        ConsoleErrorsAndWarnings(errors, warnings);
    }

    /**
     * Function to collect data from a single page.
     */
    async function collect_apsr_page_data() {
        // Check if the main results region is available.
        const main_results_region_selector = "#maincontent";
        const results_view = document.querySelector(main_results_region_selector);
        if (!results_view) {
            errors.push('Selector ' + main_results_region_selector + ' NOT found.');
            ConsoleErrorsAndWarnings(errors, warnings);
            return;
        }

        // Update the "next page" link for the next (potential) iteration.
        next_page_link = results_view.querySelector('a[aria-label="Next page"]');
        if (!next_page_link) {
            warnings.push('Next page link NOT found. Collection quantity may run short.');
            ConsoleErrorsAndWarnings(errors, warnings);
            return;
        }

        // Get references to each row of the results.
        const all_results_rows = results_view.querySelectorAll('.representation.overview.search');
        if (all_results_rows.length === 0) {
            errors.push('No search results found.');
            ConsoleErrorsAndWarnings(errors, warnings);
            return;
        }

        // Iterate over each row to collect data.
        for (let current_row of all_results_rows) {
            if (apsr_data.length >= collection_count) {
                break;
            }

            const title = NormalizeEncoding(current_row.querySelector('.title h3 a.part-link').textContent);
            const authors = NormalizeEncoding(
                Array.from(current_row.querySelectorAll('li.author a.more-by-this-author'))
                    .map(el => el.textContent.trim())
                    .join(', ')
            );


            const published_online_date = current_row.querySelector('span.date').textContent.trim();
            const cited_by_count = current_row.querySelector('a.listing-citation-modal span.number').textContent.trim();
            const article_link = current_row.querySelector('.title h3 a.part-link').href;
            let abstract = await RetrieveAbstract(current_row, title);
            let all_citing_papers_link = await RetrieveCitingPapersLink(current_row, title);

            // Push collected data to the results array.
            apsr_data.push({
                title,
                authors,
                published_online_date,
                cited_by_count,
                abstract,
                article_link,
                all_citing_papers_link
            });
        }
    }

    /**
     * Function to retrieve the abstract of a paper.
     * @param {Element} row - The current row element.
     * @param {string} title - The title of the paper.
     * @returns {Promise<string>} - The abstract text.
     */
    function RetrieveAbstract(row, title) {
        return new Promise((resolve) => {
            const abstract_link = row.querySelector('button[onclick^="toggleViewHide(this, \'abstract"]');
            if (!abstract_link) {
                warnings.push(`Abstract extraction failed for "${title}".`);
                resolve('N/A');
                return;
            }
            abstract_link.click();

            setTimeout(() => {
                const abstract_content = row.querySelector('.abstract[data-abstract-type="normal"]');
                if (!abstract_content) {
                    warnings.push(`Abstract not found for "${title}".`);
                    resolve('N/A');
                } else {
                    const abstract_text = NormalizeEncoding(abstract_content.textContent);
                    resolve(abstract_text);
                }
            }, base_load_buffer_ms);
        });
    }

    /**
     * Function to retrieve the link to all citing papers.
     * @param {Element} row - The current row element.
     * @param {string} title - The title of the paper.
     * @returns {Promise<string>} - The link to all citing papers.
     */
    function RetrieveCitingPapersLink(row, title) {
        return new Promise((resolve) => {
            const dialog_link = row.querySelector('.citation .listing-citation-modal');
            if (!dialog_link) {
                warnings.push(`Popup initiation failed for "${title}".`);
                resolve('N/A');
                return;
            }
            dialog_link.click();

            setTimeout(() => {
                const dialog_download_link = document.querySelector(
                    'a[href^="/core/services/aop-cambridge-core/download/cited-by"]'
                );
                if (!dialog_download_link) {
                    warnings.push(`Download link not found for "${title}".`);
                    resolve('N/A');
                } else {
                    const link = dialog_download_link.href;
                    resolve(link);
                }
            }, base_load_buffer_ms);
        });
    }

    /**
     * Function to log errors and warnings to the console.
     * @param {Array<string>} errors - The list of errors.
     * @param {Array<string>} warnings - The list of warnings.
     */
    function ConsoleErrorsAndWarnings(errors, warnings) {
        if (errors.length > 0) {
            console.log('\nErrors:');
            errors.forEach((error, i) => console.log(`\tError #${i}: ${error}`));
        }

        if (warnings.length > 0) {
            console.log('\nWarnings:');
            warnings.forEach((warning, i) => console.log(`\tWarning #${i}: ${warning}`));
        }
    }

    /**
     * Function to download the results as a CSV file.
     * @param {Array<Object>} jsonData - The data to be downloaded.
     * @param {string} filename - The name of the CSV file.
     */
    function DownlaodResultsAsCSV(jsonData, filename) {
        if (!jsonData.length) {
            console.warn('No data available to download.');
            return;
        }

        // Convert the JSON data to CSV format.
        const headers = Object.keys(jsonData[0]).join(',') + '\n';
        const csv_rows = jsonData.map(row =>
            Object.values(row).map(value => `"${value}"`).join(',')
        ).join('\n');
        const csv_content = headers + csv_rows;
        const blob = new Blob(["\uFEFF" + csv_content], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);

        // Auto-download the file.
        const temp_link = document.createElement('a');
        temp_link.setAttribute('href', url);
        temp_link.setAttribute('download', filename);
        document.body.appendChild(temp_link);
        temp_link.click();
        document.body.removeChild(temp_link);
    }

    // Function to normalize encoding of text.
    function NormalizeEncoding(text) {
        return text
            .normalize('NFC')
            .replace(/√ò/g, 'Ø')
            .replace(/√¶/g, 'æ')
            .replace(/√¶/g, 'å')
            .replace(/√∏/g, 'Å')
            .replace(/√º/g, 'ø')
            .replace(/√§/g, 'ß')
            .replace(/√©/g, 'é')
            .replace(/√¨/g, 'è')
            .replace(/√î/g, 'ö')
            .replace(/√ñ/g, 'ü')
            .replace(/√Ä/g, 'ä')
            .replace(/√á/g, 'á')
            .replace(/√í/g, 'í')
            .replace(/√ó/g, 'ó')
            .replace(/√ú/g, 'ú')
            .replace(/√à/g, 'à')
            .replace(/√ù/g, 'ù')
            .replace(/√â/g, 'â')
            .replace(/√ê/g, 'ê')
            .replace(/√î/g, 'î')
            .replace(/√ô/g, 'ô')
            .replace(/√û/g, 'û')
            .replace(/√ë/g, 'ë')
            .replace(/√ï/g, 'ï')
            .replace(/√ÿ/g, 'ÿ')
            .replace(/√æ/g, 'æ')
            .replace(/√Æ/g, 'Æ')
            .replace(/√Ø/g, 'Ø')
            .replace(/√ö/g, 'ö')
            .replace(/\r?\n|\r|\t/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    // Clear the console and start the data collection process.
    clear();
    collect_apsr_data();
}

// Start the automation process with a specified collection count.
automate_apsr(SEARCH_RESULT_COLLECTION_COUNT);
