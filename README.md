# Streamlit Snowflake Connector App

This Streamlit application allows users to upload a CSV file, process the data, and save it to a Snowflake database. Users can choose to connect to Snowflake using credentials from a JSON file or by manually entering them.

## Prerequisites

Before running the app, ensure you have the following installed:

- Python 3.7 or later
- pip (Python package manager)

### Required Python Packages

Install the required packages using pip:

```bash
pip install streamlit pandas snowflake-connector-python
```

### Snowflake Credentials

Create a `creds-snowflake.json` file in the root directory with the following format:

```json
{
    "account": "your_account",
    "user": "your_username",
    "password": "your_password",
    "database": "your_database",
    "schema": "your_schema"
}
```

**Note:** Ensure this file is included in your `.gitignore` to prevent it from being committed to version control.

## Usage

1. **Run the Streamlit App:**

   Start the Streamlit app by running the following command in your terminal:

   ```bash
   streamlit run main.py
   ```

2. **Upload a CSV File:**

   - Use the file uploader in the app to select a CSV file from your local machine.

3. **Connect to Snowflake:**

   - Choose the connection method from the sidebar:
     - **Use JSON File:** Automatically loads credentials from `creds-snowflake.json`.
     - **Manual Input:** Enter your Snowflake credentials manually in the sidebar.

4. **Save Data to Snowflake:**

   - The app will display the uploaded file name as the default table name (greyed out).
   - Optionally, enter a custom table name.
   - Click the "Save to Snowflake" button to create the table (if it doesn't exist) and save the data.

## Features

- **Automatic Table Creation:** The app will create a Snowflake table based on the DataFrame schema if it doesn't exist.
- **Data Type Mapping:** Automatically maps pandas data types to Snowflake data types.
- **Secure Connection:** Supports both JSON file and manual input for Snowflake credentials.


## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

