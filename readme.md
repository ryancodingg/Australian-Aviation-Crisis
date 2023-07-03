# PACE - EY Data and Analytics - Groups 21 & 22

## Overview

EY Data & Analytics team was approached by airport admins and airline CEOs with the goal of building a report covering the current aviation crisis in Australia.​

Students will be required to analyse the data provided and build an end-to-end solution to allow the ingestion of data into a model, which will be fed into a centralised analytics dashboard.

This dashboard must provide insight into the current issues related to airport/flight delays, busiest routes, safety, etc and must address a set of business requirements that will be handed to them.

### Setup

0. *(Optional)* For non-technical team members we also hesitantly reccomend [SourceTree](https://www.sourcetreeapp.com/) as it gives them a nice UI to interact with git although please remember that this tool is not common in industry and developing your git skills with command line is invaluable.
*If you do this step you can ignore most other steps as SourceTree will walk you through downloading the repository and relevant branches*

1. Clone the Template Repo
Clone a local copy of the repo onto your machine and cd into the project directory.

```bash
git clone https://github.com/EY-PACE/PACE-S1-2023.git
cd <git-repo-name>
```

If you struggle with git we reccomend you read through [Atlassian's Getting Started tutorial](https://www.atlassian.com/git/tutorials/setting-up-a-repository).

### Engineering
2. Fetch and checkout the Engineering branch that you will use to upload your work.

```bash
git fetch && git checkout engineering
```

Here you can store and version control any scripts and configuration files *(ensure you don't include secrets in these uploaded files)* and workflow/pipeline documentation/diagrams that are used in the creation of engineering pipelines within your cloud infrastructure.
