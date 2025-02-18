import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

# Set page title
st.title("Welcome App")

# Add connection method selection
connection_method = st.sidebar.radio(
    "Choose connection method",
    ["Use JSON File", "Manual Input"]
)

# Initialize connection state
if 'snowflake_conn' not in st.session_state:
    st.session_state.snowflake_conn = None

if connection_method == "Use JSON File":
    try:
        with open('creds-snowflake.json') as f:
            creds = json.load(f)
            snowflake_account = creds.get('account')
            snowflake_user = creds.get('user')
            snowflake_password = creds.get('password')
            snowflake_database = creds.get('database')
            snowflake_schema = creds.get('schema')
            st.sidebar.success("Successfully loaded credentials from JSON file")
    except Exception as e:
        st.sidebar.error(f"Error loading JSON file: {str(e)}")
else:
    # Manual input fields
    st.sidebar.header("Snowflake Connection")
    snowflake_account = st.sidebar.text_input("Snowflake Account", type="password")
    snowflake_user = st.sidebar.text_input("Username", type="default")
    snowflake_password = st.sidebar.text_input("Password", type="password")
    snowflake_database = st.sidebar.text_input("Database", type="default")
    snowflake_schema = st.sidebar.text_input("Schema", type="default")

# Connect to Snowflake button
if st.sidebar.button("Connect to Snowflake"):
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            database=snowflake_database,
            schema=snowflake_schema
        )
        st.session_state.snowflake_conn = conn
        st.sidebar.success("Successfully connected to Snowflake!")
    except Exception as e:
        st.sidebar.error(f"Connection failed: {str(e)}")

# Add file uploader for CSV
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

# Check if a file has been uploaded
if uploaded_file is not None:
    # Read the CSV file
    df = pd.read_csv(uploaded_file)
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Display the first few rows of the dataset
    st.write("### Preview of your data:")
    st.dataframe(df.head())
    
    # Add date range slider
    st.write("### Select Date Range")
    min_date = df['Date'].min().to_pydatetime()
    max_date = df['Date'].max().to_pydatetime()
    
    # Create a date range slider
    start_date, end_date = st.select_slider(
        'Select date range',
        options=df['Date'].dt.date.unique(),
        value=(min_date.date(), max_date.date())
    )
    
    # Convert dates back to datetime for filtering
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Filter the dataframe based on selected dates
    mask = (df['Date'] >= start_date) & (df['Date'] <= end_date)
    filtered_df = df.loc[mask]
    
    # Create two columns for the charts
    st.write("### Price and Volume Charts")
    
    # Plot price chart with filtered data
    st.line_chart(
        data=filtered_df.set_index('Date')['Close'],
        use_container_width=True
    )
    
    # Plot volume chart with filtered data
    st.area_chart(
        data=filtered_df.set_index('Date')['Volume'],
        use_container_width=True
    )

    # After processing the data, add option to save to Snowflake
    if uploaded_file is not None and st.session_state.snowflake_conn is not None:
        st.write("### Save to Snowflake")
        # Get filename without extension and replace spaces with underscores
        default_table_name = uploaded_file.name.rsplit('.', 1)[0].replace(' ', '_')
        
        # Show the default table name in a disabled text input
        st.text_input("Default table name", value=default_table_name, disabled=True)
        # Allow user to input custom table name
        table_name = st.text_input("Enter custom table name (optional)", value=default_table_name)
        
        if st.button("Save to Snowflake"):
            try:
                # Create cursor
                cursor = st.session_state.snowflake_conn.cursor()
                
                # Generate CREATE TABLE statement based on DataFrame schema
                columns = []
                for column, dtype in filtered_df.dtypes.items():
                    # Map pandas datatypes to Snowflake datatypes
                    if dtype == 'datetime64[ns]':
                        sf_type = 'TIMESTAMP'
                    elif dtype == 'float64':
                        sf_type = 'FLOAT'
                    elif dtype == 'int64':
                        sf_type = 'INTEGER'
                    else:
                        sf_type = 'VARCHAR'
                    columns.append(f'"{column}" {sf_type}')
                
                # Format table name for Snowflake (using proper quoting)
                formatted_table_name = f'"{snowflake_database}"."{snowflake_schema}"."{table_name.upper()}"'
                
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {formatted_table_name} (
                    {', '.join(columns)}
                )
                """
                
                # Execute CREATE TABLE statement
                cursor.execute(create_table_sql)
                
                # Write the filtered dataframe to Snowflake
                success, nchunks, nrows, _ = write_pandas(
                    conn=st.session_state.snowflake_conn,
                    df=filtered_df,
                    table_name=table_name.upper(),  # Snowflake defaults to uppercase
                    database=snowflake_database,
                    schema=snowflake_schema
                )
                st.success(f"Successfully wrote {nrows} rows to {table_name}")
                
                # Close cursor
                cursor.close()
                
            except Exception as e:
                st.error(f"Error writing to Snowflake: {str(e)}")
