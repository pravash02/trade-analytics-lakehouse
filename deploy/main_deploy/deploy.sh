echo "Env = $ENV"

function init(){
    echo "Initializing deployment environment..."
    if [ -z "$ENV" ]; then
        echo "Error: ENV variable is not set. Please set it to the target environment (e.g., dev, staging, prod)."
        exit 1
    fi
    echo "Deployment environment: $ENV"
}


function main(){
    init
    python3 --version
    python3 -m pip install --user -r requirements.txt
    
    export DATABRICKS_HOST=$(python3 -c "import config; config.get_host('$ENV')")
    echo "Host = $DATABRICKS_HOST"
    echo "Authenticating ..."
    export DATABRICKS_TOKEN=$(python3 -c "import config; config.get_token('$ENV')")
    echo "Deploying to $ENV environment ..."
    ./databricks bundle deploy -t ENV
}

main 