install_my_app(){
    echo $?
    cd $install_dir/deploy/main_deploy
    chmod a+x -R *
    echo `pwd`
    echo "Env: $ENV"
    ./deploy.sh $ENV
}

start(){
    echo $?
}

stop(){
    echo $?
}

check(){
    echo $?
}