# Care.com Scraper

## Project Overview
This project extracts caregiver data from Care.com using its GraphQL API. The scraper fetches details such as name, location, experience, and hourly rates.

## How It Works
1. **Finding the API Endpoint:**  
   - Used browser DevTools to inspect network requests.
   - Identified the GraphQL API endpoint.

2. **Request Extraction:**  
   - Copied the network request as a cURL command.
   - Imported it into Postman.
   - Generated a Python `requests` script from Postman.

3. **Understanding Pagination:**  
   - Identified the `searchAfter` property in the request payload.
   - Discovered that `searchAfter` is a Base64-encoded value corresponding to `endCursor` in the response.
   - Decoded `searchAfter`, revealing a pattern containing:
     ```
     Seed=0, Index=40, PageScrollId=d275cf0728014bf0bef285fc19c3ebcf
     ```
   - This allows iterating through pages by passing the latest `endCursor` value from the response into the next request.

4. **Code Organization:**  
   - Moved authentication data (cookies) to `config.py` for security.
   - Created a `main.py` script to send requests and parse responses.
   - Designed a `playground.ipynb` notebook for testing.

## Rate Limiting
Initially, I will test the scraper without implementing any rate-limiting measures. If I encounter issues such as IP blocks or throttling, we will consider:
1. **Rotating User Agents:** Use different `User-Agent` headers to mimic various devices.
2. **Using Proxies or Tor:** Rotate IP addresses via paid proxy services or the Tor network.
3. **Delays Between Requests:** Implement `time.sleep()` with random intervals.
4. **GraphQL Query Optimization:** Request only necessary fields to minimize server load.

## Running the Code
1. Ensure `config.py` contains the necessary authentication details (cookie).
2. Run the script:
   ```bash
   python main.py
   ```
3. Alternatively, run it in Jupyter Notebook:
    ```python
    from main import fetch_profiles
    try:
       result = fetch_profiles()
       print(json.dumps(result, indent=2))
    except Exception as e:
       print(f"Error: {e}")
    ```
