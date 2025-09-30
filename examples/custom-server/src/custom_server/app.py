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
                "status": app.app_status.message,
                "app_url": app.url,
                "source_code_path": app.active_deployment.source_code_path
            }
            app_list.append(app_info)
        
        return app_list
    except Exception as e:
        return [{"error": f"Failed to list apps: {str(e)}"}]


mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)
