# Dataiku DSS Plugin - Datasets about Dataiku Instance

This plugin is part of the [Upgrade Planning Toolkit](https://github.com/THE-MOLECULAR-MAN/dss_project_DSS_Upgrade_Planning_Toolkit/tree/v12.3.2_to_v14.3.2), which helps customers sift through hundreds to thousands of projects on a DSS Design node to automate the discovery of objects that may require attention during an upgrade.

This version of the plugin is intended for DSS v12.3 and later. It does not provide as many features the v14 branch, and lacks resources that weren't introduced until later DSS versions such as agents, agent tools, and LLMs. This branch for DSS v12 and v13 is also slower to run than the v14 plugin due to limitations of the Dataiku Python API that were resolved in later versions of DSS.

This plugin is not officially supported by Dataiku. It should only be used on Design nodes, not automation or API nodes, and should not be used in any mission critical environments.


## Overview

This Dataiku DSS plugin provides read-only datasets that describe metadata about Dataiku resources on the current node:
* Apps
* Agents
* Clusters
* Code Environments
* Connections
* Datasets
* Meanings
* Plugins
* Projects
* Recipes
* Users
* Web Apps

Intended use cases:
* assisting with planning upgrades/migratins by locating soon to be deprecated items (recipes, connections, plugins, clusters, etc)
* monitoring usage
* locating empty projects
* locating hardcoded database names, schemas in datasets that should be migrated to use variables
* quickly generating exportable lists of information for auditors
* making information about a Dataiku instance available for querying via natural language in a Dataiku agent
* maintanence - locating unused code environments that take up large amounts of disk space


## Installation

Follow these steps:

1. Download the [Source code (zip) from GitHub](https://github.com/THE-MOLECULAR-MAN/dss-plugin-xzibit/releases).
2. In your Dataiku instance, navigate Plugins > Add plugin > Upload and select the downloaded plugin package.
3. Follow the on-screen instructions to complete the installation.

## Usage

### Dataset Usage

1. **Add Item > Connect or create > Datasets about Dataiku Instance**: In your Dataiku project, add a new dataset
2. **Select the type of internal dataset**:


## Permissions and access
This plugin provides datasets using Dataiku's Python API. The datasets produce a list of all resources that are visible to the user who builds the workflow. This plugin is designed to be used by Dataiku Administrators and not users with limited access.

## Limitations

**Potential role based access control issues**
- These datasets are generated using the user's permissions. If this plugin's datasets are pushed into other datasets, then metadata about those other resources (datasets, recipes, models, etc) may be visible by users who would not normally be able to see those resources.

## Support

For any issues or feature requests, please contact the plugin maintainer or open an issue on the [plugin's GitHub repository](https://github.com/THE-MOLECULAR-MAN/dss-plugin-xzibit/).
