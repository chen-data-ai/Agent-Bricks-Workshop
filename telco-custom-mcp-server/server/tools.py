"""
Tools module for the MCP server.

This module defines all the tools (functions) that the MCP server exposes to clients.
Tools are the core functionality of an MCP server - they are callable functions that
AI assistants and other clients can invoke to perform specific actions.

Each tool should:
- Have a clear, descriptive name
- Include comprehensive docstrings (used by AI to understand when to call the tool)
- Return structured data (typically dict or list)
- Handle errors gracefully
"""

from server import utils
from databricks.sdk import WorkspaceClient

# Initialize Databricks client
workspace_client = WorkspaceClient()

def load_tools(mcp_server):
    """
    Register all MCP tools with the server.

    This function is called during server initialization to register all available
    tools with the MCP server instance. Tools are registered using the @mcp_server.tool
    decorator, which makes them available to clients via the MCP protocol.

    Args:
        mcp_server: The FastMCP server instance to register tools with. This is the
                   main server object that handles tool registration and routing.

    Example:
        To add a new tool, define it within this function using the decorator:

        @mcp_server.tool
        def my_new_tool(param: str) -> dict:
            '''Description of what the tool does.'''
            return {"result": f"Processed {param}"}
    """

    @mcp_server.tool
    def health() -> dict:
        """
        Check the health of the MCP server and Databricks connection.

        This is a simple diagnostic tool that confirms the server is running properly.
        It's useful for:
        - Monitoring and health checks
        - Testing the MCP connection
        - Verifying the server is responsive

        Returns:
            dict: A dictionary containing:
                - status (str): The health status ("healthy" if operational)
                - message (str): A human-readable status message

        Example response:
            {
                "status": "healthy",
                "message": "Custom MCP Server is healthy and connected to Databricks Apps."
            }
        """
        return {
            "status": "healthy",
            "message": "Custom MCP Server is healthy and connected to Databricks Apps.",
        }

    @mcp_server.tool
    def get_current_user() -> dict:
        """
        Get information about the current authenticated user.

        This tool retrieves details about the user who is currently authenticated
        with the MCP server. When deployed as a Databricks App, this returns
        information about the end user making the request. When running locally,
        it returns information about the developer's Databricks identity.

        Useful for:
        - Personalizing responses based on the user
        - Authorization checks
        - Audit logging
        - User-specific operations

        Returns:
            dict: A dictionary containing:
                - display_name (str): The user's display name
                - user_name (str): The user's username/email
                - active (bool): Whether the user account is active

        Example response:
            {
                "display_name": "John Doe",
                "user_name": "john.doe@example.com",
                "active": true
            }

        Raises:
            Returns error dict if authentication fails or user info cannot be retrieved.
        """
        try:
            w = utils.get_user_authenticated_workspace_client()
            user = w.current_user.me()
            return {
                "display_name": user.display_name,
                "user_name": user.user_name,
                "active": user.active,
            }
        except Exception as e:
            return {"error": str(e), "message": "Failed to retrieve user information"}

    """
    TODO: Add more tools as necessary
    """
    @mcp_server.tool
    def get_latest_return() -> dict:
        """
        Returns the most recent customer service interaction, such as returns.

        Raises:
            Returns error dict if authentication fails or user info cannot be retrieved.
        """
        try:
            # Call Unity Catalog function to get the latest customer service interaction
            sql_statement = f"""
            SELECT 
                CAST(date_time AS DATE) AS purchase_date,
                issue_category,
                issue_description,
                name
            FROM stable_classic_6xeo9l_catalog.data_schema.cust_service_data
            ORDER BY date_time DESC
            LIMIT 1
            """
    
            result = workspace_client.statement_execution.execute_statement(
                    warehouse_id="005d0025bf70a3e1",
                    statement=sql_statement
             )
            
            return {
                "latest_return": result.result.data_array
            }
        except Exception as e:
            return {"error": str(e), "message": "Failed to retrieve customer service interaction"}
        
    @mcp_server.tool
    def get_return_policy() -> dict:
        """
        Returns the most recent company return and exchange policy. 

        Raises:
            Returns error dict if authentication fails or user info cannot be retrieved.
        """
        try:
            # Call Unity Catalog function to return company policy
            sql_statement = f"""
            SELECT
                policy,
                policy_details,
                last_updated
            FROM stable_classic_6xeo9l_catalog.data_schema.policies
            WHERE policy = 'Device Return and Exchange Policy'
            LIMIT 1
            """
    
            result = workspace_client.statement_execution.execute_statement(
                    warehouse_id="005d0025bf70a3e1",
                    statement=sql_statement
             )
            
            return {
                "return_policy": result.result.data_array
            }
        except Exception as e:
            return {"error": str(e), "message": "Failed to retrieve company policy"}
        
    @mcp_server.tool
    def get_invoice_details() -> dict:
        """
        Returns information about invoice details.  

        Raises:
            Returns error dict if authentication fails or user info cannot be retrieved.
        """
        try:
            # Call Unity Catalog function to return company policy
            sql_statement = f"""
            WITH parsed_invoice AS (
                SELECT
                    path,
                    content
                FROM
                READ_FILES('/Volumes/stable_classic_6xeo9l_catalog/data_schema/invoice', format => 'binaryFile')
            ),
            raw_data AS (
                SELECT
                    path,
                    ai_parse_document(content) AS parsed
                FROM
                parsed_invoice
                ),
            -- Step 2: Extract all text content from the parsed document
            extracted_content AS (
                SELECT
                    path,
                    -- Concatenate all text elements into full document text
                    concat_ws(
                    '\n\n',
                    transform(
                    filter(
                    try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                    element -> try_cast(element:type AS STRING) IN ('text', 'section_header', 'title')
                    )   ,
                    element -> try_cast(element:content AS STRING)
                    )
                    ) AS full_text,
                    -- Extract tables separately (pricing, line items)
                    transform(
                    filter(
                        try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                        element -> try_cast(element:type AS STRING) = 'table'
                    ),
                    element -> try_cast(element:content AS STRING)
                    ) AS tables,
                    -- Get page count
                    try_cast(parsed:document:page_count AS INT) AS page_count,
                    -- Check for errors
                    try_cast(parsed:error_status AS STRING) AS parse_error
                FROM raw_data
            )

                Select 
                    ai_extract(full_text, 
                    array('Customer name', 'Customer address', 'Invoice number', 'Invoice date', 'Total amount')) as extracted_fields,
                    ai_query('databricks-meta-llama-3-1-8b-instruct', 
                    CONCAT(format_string('Summarize the transaction details in 1 sentence. Transaction details: '), `full_text`)) as summary,
                    full_text, 
                    tables
                from extracted_content
            """
    
            result = workspace_client.statement_execution.execute_statement(
                    warehouse_id="005d0025bf70a3e1",
                    statement=sql_statement,
                    wait_timeout = "50s"
             )
            
            return {
                "return_policy": result.result.data_array
            }
        except Exception as e:
            return {"error": str(e), "message": "Failed to retrieve invoice info"}

