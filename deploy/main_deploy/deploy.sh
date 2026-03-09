readonly WORKING_DIR="$(pwd -P)"
echo "Env = $ENV"

function init(){
    export TERRAFORM_PATH=""

    DBX_CLI_VER=0.217.1
    DBX_CLI_ZIP=databricks-cli-${DBX_CLI_VER}_linux_amd64.zip
    DBX_CLI_URL=https://github.com/databricks/databricks-cli/releases/download/v${DBX_CLI_VER}/${DBX_CLI_ZIP}

    rm -rd tmp
    mkdir tmp
    cd tmp

    curl -LOs $DBX_CLI_URL
    python3 -c "from zipfile import PyZipFile; PyZipFile('$DBX_CLI_ZIP').extractall()";
    cp databricks ../
    cd ..
    rm -rf tmp
    chmod +x databricks
    ./databricks --version

    cp databricks ../../
    cp config.py ../../
    cd ../../

    match="exec_path:.*"
    insert="exec_path: $TERRAFORM_PATH"
    sed -i "s|$match|$insert|" databricks.yml

    mkdir -p deploy/main_deploy/TARGET
    env_settings_yaml=deploy/targets/$ENV/settings.yml
    target_settings_yaml=deploy/targets/TARGET/settings.yml
    awk '{print}' $env_settings_yaml > $target_settings_yaml

    echo "Init Done"
}


function main(){
    init
    python3 --version
    python3 -m pip install --user -r requirements.txt
    
    export DATABRICKS_HOST=$(python3 -c "import config; config.get_host('TARGET')")
    echo "Host = $DATABRICKS_HOST"
    echo "Authenticating ..."
    export DATABRICKS_TOKEN=$(python3 -c "import config; config.get_token('TARGET')")
    echo "Deploying to TARGET environment ..."
    ./databricks bundle deploy -t TARGET
}

main 