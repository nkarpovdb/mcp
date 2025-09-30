from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient

STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Custom MCP Server on Databricks Apps")


# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


# Add a tool to list Databricks apps
@mcp.tool()
def list_databricks_apps() -> list[dict]:
    """List all Databricks apps in the workspace"""
    try:
        w = WorkspaceClient()
        apps = w.apps.list()
        
        app_list = []
        for app in apps:
            app_info = {
                "name": app.name,
                "description": app.description,
                "app_url": app.url,
                "source_code_path": app.active_deployment.source_code_path
            }
            app_list.append(app_info)
        
        return app_list
    except Exception as e:
        return [{"error": f"Failed to list apps: {str(e)}"}]


# Add a tool to download workspace files
@mcp.tool()
def download_workspace_file(path: str) -> dict:
    """Download the contents of a file from the Databricks workspace
    
    Args:
        path: Absolute workspace path to the file (e.g., "/Workspace/Users/user@company.com/my_notebook")
    
    Returns:
        Dict containing file contents and metadata
    """
    try:
        w = WorkspaceClient()
        
        # Download the file content
        file_obj = w.workspace.download(path)
        content = file_obj.read()
        
        # Try to decode as text, fallback to base64 for binary files
        try:
            text_content = content.decode('utf-8')
            is_binary = False
        except UnicodeDecodeError:
            import base64
            text_content = base64.b64encode(content).decode('ascii')
            is_binary = True
        
        return {
            "path": path,
            "content": text_content,
            "is_binary": is_binary,
            "size_bytes": len(content),
            "success": True
        }
    except Exception as e:
        return {
            "path": path,
            "error": f"Failed to download file: {str(e)}",
            "success": False
        }


# Add a tool to redeploy/restart a Databricks app
@mcp.tool()
def redeploy_databricks_app(app_name: str, source_code_path: str = None) -> dict:
    """Redeploy (restart) a Databricks app by creating a new deployment
    
    Args:
        app_name: Name of the app to redeploy
        source_code_path: Optional path to source code for new deployment
    
    Returns:
        Dict containing deployment status and metadata
    """
    try:
        w = WorkspaceClient()
        
        # Get the app to verify it exists and get current deployment info
        try:
            app = w.apps.get(app_name)
        except Exception as e:
            return {
                "app_name": app_name,
                "error": f"App not found: {str(e)}",
                "success": False
            }
        
        # Use current source code path if not provided
        if source_code_path is None and app.active_deployment:
            source_code_path = app.active_deployment.source_code_path
        
        if not source_code_path:
            return {
                "app_name": app_name,
                "error": "No source code path available for deployment",
                "success": False
            }
        
        # Create new deployment (this effectively redeploys the app)
        deployment = w.apps.deploy(
            app_name=app_name,
            source_code_path=source_code_path
        )
        
        return {
            "app_name": app_name,
            "deployment_id": deployment.deployment_id,
            "source_code_path": source_code_path,
            "status": deployment.status.value if deployment.status else "unknown",
            "success": True
        }
    except Exception as e:
        return {
            "app_name": app_name,
            "error": f"Failed to redeploy app: {str(e)}",
            "success": False
        }


# Add a tool to start a stopped Databricks app
@mcp.tool()
def start_databricks_app(app_name: str) -> dict:
    """Start a stopped Databricks app
    
    Args:
        app_name: Name of the app to start
    
    Returns:
        Dict containing start status and metadata
    """
    try:
        w = WorkspaceClient()
        
        # Start the app
        w.apps.start(app_name)
        
        # Get updated app info
        app = w.apps.get(app_name)
        
        return {
            "app_name": app_name,
            "status": app.compute_status.value if app.compute_status else "unknown",
            "app_url": app.url,
            "success": True
        }
    except Exception as e:
        return {
            "app_name": app_name,
            "error": f"Failed to start app: {str(e)}",
            "success": False
        }


# Add a tool to stop a running Databricks app
@mcp.tool()
def stop_databricks_app(app_name: str) -> dict:
    """Stop a running Databricks app
    
    Args:
        app_name: Name of the app to stop
    
    Returns:
        Dict containing stop status and metadata
    """
    try:
        w = WorkspaceClient()
        
        # Stop the app
        w.apps.stop(app_name)
        
        # Get updated app info
        app = w.apps.get(app_name)
        
        return {
            "app_name": app_name,
            "status": app.compute_status.value if app.compute_status else "unknown",
            "success": True
        }
    except Exception as e:
        return {
            "app_name": app_name,
            "error": f"Failed to stop app: {str(e)}",
            "success": False
        }


# Add a tool to upload workspace files
@mcp.tool()
def upload_workspace_file(path: str, content: str, is_binary: bool = False, overwrite: bool = True, language: str = None) -> dict:
    """Upload content to a file in the Databricks workspace
    
    Args:
        path: Absolute workspace path where to save the file (e.g., "/Workspace/Users/user@company.com/my_file.py")
        content: File content as string (base64 encoded if is_binary=True)
        is_binary: Whether the content is binary and base64 encoded
        overwrite: Whether to overwrite existing file (default: True)
        language: Language for notebooks (PYTHON, SQL, SCALA, R) - optional
    
    Returns:
        Dict containing upload status and metadata
    """
    try:
        w = WorkspaceClient()
        
        # Handle binary content
        if is_binary:
            import base64
            content_bytes = base64.b64decode(content)
        else:
            content_bytes = content.encode('utf-8')
        
        # Determine format and language
        from databricks.sdk.service.workspace import ImportFormat, Language
        
        # Default format
        format = ImportFormat.SOURCE
        
        # Set language if specified
        language_enum = None
        if language:
            language_upper = language.upper()
            if hasattr(Language, language_upper):
                language_enum = getattr(Language, language_upper)
        
        # Upload the file
        w.workspace.upload(
            path=path,
            content=content_bytes,
            format=format,
            language=language_enum,
            overwrite=overwrite
        )
        
        return {
            "path": path,
            "size_bytes": len(content_bytes),
            "overwrite": overwrite,
            "language": language,
            "success": True
        }
    except Exception as e:
        return {
            "path": path,
            "error": f"Failed to upload file: {str(e)}",
            "success": False
        }


mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)
