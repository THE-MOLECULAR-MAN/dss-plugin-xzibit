# Dataiku DSS Plugin - Datasets about Dataiku Instance

## Overview

This version of the plugin is intended for **DSS version 14.3** and later.

This Dataiku DSS plugin provides read-only datasets to various Dataiku objects:

* Agents
* Agent Tools
* Agent Tool Types
* Apps
* Clusters
* Code Environments
* Connections
* Datasets
* Knowledge Banks
* LLM models by connection
* LLM usage
* Meanings
* Plugins
* Plugin usage/instances
* Projects
* Recipes
* Retrieval Augmented LLMs
* Users
* Web Apps

## Use cases:
* migration planning by locating deprecated items (recipes, connections, plugins, clusters, etc)
* migration planning by locating which scenarios and projects are in use
* Enforcing best practices
* quickly generating exportable lists of information for auditors
* making information about a Dataiku instance available for querying via natural language in a Dataiku agent
* maintanence - locating unused code environments that take up large amounts of disk space

## Installation

Follow these steps:

1. Download the [Source code (zip) from GitHub](https://github.com/THE-MOLECULAR-MAN/dss-plugin-xzibit/releases).
2. In your Dataiku instance, navigate Plugins > Add plugin > Upload and select the downloaded plugin package.
3. Follow the on-screen instructions to complete the installation.

## Usage

### Using the datasets

1. **Add Item > Connect or create > Datasets about Dataiku Instance**: In your Dataiku project, add a new dataset
2. **Select the type of internal dataset**:

## Limitations

**Potential role based access control issues**
- These datasets are generated using the user's permissions. If the data is stored into another dataset, then it may be visible by users who should not be able to access it.

## Support

For any issues or feature requests, please contact the plugin maintainer or open an issue on the [plugin's GitHub repository](https://github.com/THE-MOLECULAR-MAN/dss-plugin-xzibit/).
