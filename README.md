# Databricks Git Integration Setup

This repository is configured to integrate with Databricks Git folders for efficient collaboration and version control of Databricks notebooks, scripts, and assets.

## Prerequisites
- **Databricks Workspace**: Access to a Databricks workspace with admin or contributor permissions.
- **Git Provider**: A Git account (e.g., GitHub, GitLab, Bitbucket, Azure DevOps).
- **Databricks CLI**: Installed and configured (see [Databricks CLI setup](https://docs.databricks.com/dev-tools/cli/index.html)).
- **Personal Access Token (PAT)**: Generate a PAT from your Git provider for authentication.
- **Python 3.8+**: For running scripts and managing dependencies.
- **Databricks Git Integration**: Ensure your workspace has Git integration enabled (admin settings).

## Setup Instructions

### 1. Clone the Repository
Clone this repository to your local machine:
```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 2. Configure Databricks CLI
Authenticate the Databricks CLI with your workspace:
```bash
databricks configure --token
```
- Enter your Databricks workspace URL (e.g., `https://<your-databricks-instance>`).
- Provide your Databricks personal access token (generate from User Settings in Databricks).

### 3. Set Up Git Integration in Databricks
1. In the Databricks workspace, navigate to **User Settings** > **Git Integration**.
2. Configure:
   - **Git Provider**: Select your provider (e.g., GitHub).
   - **Git Username**: Your Git account username.
   - **Personal Access Token**: Paste your Git provider PAT.
3. Save the settings.

### 4. Link Repository to Databricks
1. In Databricks, go to **Repos** in the workspace sidebar.
2. Click **Add Repo** and select **Create Repo**.
3. Provide:
   - **Repository URL**: `https://github.com/<your-username>/<your-repo>.git`.
   - **Provider**: Select your Git provider.
4. Clone the repo into your Databricks workspace.

### 5. Folder Structure
The repository is structured to align with Databricks Git folders:
```
<your-repo>/
├── notebooks/                # Databricks notebooks (.py, .ipynb, .sql, .r)
├── scripts/                  # Python/SQL scripts for jobs or utilities
├── data/                     # Sample datasets or references (avoid sensitive data)
├── tests/                    # Unit tests for scripts and pipelines
├── configs/                  # Configuration files (e.g., job configs, cluster settings)
└── README.md                 # This file
```

### 6. Working with Notebooks
- **Add Notebooks**: Create or import notebooks in the `notebooks/` folder via Databricks UI or CLI.
- **Commit Changes**:
  1. In Databricks, use the **Repos** UI to stage and commit changes.
  2. Or, locally:
     ```bash
     git add .
     git commit -m "Add new notebook"
     git push origin main
     ```
- **Pull Updates**: In Databricks Repos, click **Pull** to sync changes from the remote repository.

### 7. Automating Workflows
- Use **Databricks Workflows** to schedule jobs referencing scripts in `scripts/`.
- Example job configuration (`configs/job_config.json`):
  ```json
  {
    "name": "sample_job",
    "tasks": [
      {
        "task_key": "run_script",
        "notebook_task": {
          "notebook_path": "/Repos/<your-username>/<your-repo>/notebooks/sample_notebook"
        }
      }
    ]
  }
  ```
- Deploy jobs using Databricks CLI:
  ```bash
  databricks jobs create --json-file configs/job_config.json
  ```

### 8. Testing
- Add unit tests in the `tests/` folder using `pytest` or Databricks' testing frameworks.
- Run tests locally:
  ```bash
  pip install pytest
  pytest tests/
  ```

### 9. Best Practices
- **Version Control**: Commit frequently with meaningful messages.
- **Sensitive Data**: Avoid storing credentials or sensitive data in the repo. Use Databricks Secrets.
- **Branching**: Use branches for features or experiments (e.g., `feature/new-pipeline`).
- **CI/CD**: Integrate with GitHub Actions or Azure DevOps for automated testing and deployment.

## Troubleshooting
- **Authentication Issues**: Verify your Git PAT and Databricks token are valid.
- **Sync Errors**: Ensure no merge conflicts exist; use `git pull` or Databricks Repos UI to resolve.
- **Permissions**: Confirm you have write access to the repo and Databricks workspace.

## Resources
- [Databricks Git Integration Docs](https://docs.databricks.com/repos/index.html)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Git Best Practices](https://git-scm.com/book/en/v2)

For questions, contact the repository maintainer or open an issue.