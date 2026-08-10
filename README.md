# Agent Bricks Workshop

This repository contains all the demo assets and notebooks for the Agent Bricks workshop, as presented at the [Databricks Generative AI Workshop](https://events.databricks.com/FY26-EV-AEB-Hands-on-Workshop-GenerativeAI)[1]. The repo is designed to help participants follow along and practice building, optimizing, and deploying practical AI agent solutions using Agent Bricks on Databricks.

## What's Here

You will find four notebooks:
- **AI Functions with Batch Inference:** Sample code demonstrating how to use AI functions such as `ai_parse_document` for information extraction.
- **Model Context Protocol (MCP) on Databricks:** Build a managed MCP tool (a Unity Catalog function), then host an external weather MCP server as a Databricks App and route it through the **Unity AI Gateway** — governed, audited, and added as a tool in the Multi-Agent Supervisor. (See `02_MCP on Databricks.ipynb` and `weather-mcp-server/`.)
- **Knowledge Assistant with Agent Bricks:** Creating a document-based question-answering assistant.
- **MLflow3 UI with the Knowledge Assistant Agent:** 

## How to Use in Databricks Workspace

### Clone or Import the Repository
- Go to your Databricks workspace.
- In the left sidebar, find the **Repos** section (sometimes called "Git folders").
- Click **Add Repo** or **Clone Repo**.
- Enter the URL:  
  https://github.com/chen-data-ai/Agent-Bricks-Workshop.git

**Requirements:**  
- Databricks workspace with Agent Bricks enabled  
- Serverless compute and Unity Catalog activated

