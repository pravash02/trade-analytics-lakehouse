# trade-analytics-lakehouse
End-to-end Lakehouse pipeline ingesting 50K+ financial trade events via Kafka simulation, transforming through Medallion architecture on Databricks + Delta Lake with dbt-databricks, orchestrated by Airflow, with live analytics dashboard.


# macOS / Linux
chmod +x setup.sh && ./setup.sh

# Windows (PowerShell as Administrator)
.\setup.ps1

# Already have Python 3.12+?
python local_setup.py


# Below setup is for MAC

## JAVA Set up
Java version - 17

1. install: brew install openjdk@17
2. symlink: sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk
3. Check if Java setup is correct: 
    - export JAVA_HOME=\$(/usr/libexec/java_home)
    - echo $JAVA_HOME
4. Check all java verions installed: /usr/libexec/java_home -V


## Spark
version - 3.5.3


## Set up in Local
vi ~/.zshrc      
vi ~/.bash_profile

Install Pytohn: `brew install python@3.12`

Add the path to ~/.zshrc: `echo 'export PATH="/path/to/python/bin:$PATH"' >> ~/.zshrc`
Apply Changes: `source ~/.zshrc`

Go into Project Folder: `cd trade_analytics-lakehouse`

Create Environment: `python3.12 -m venv .venv`

Activate Environment: `source .venv/bin/activate`

Install dependencies: `pip install -e ".[dev]"`
