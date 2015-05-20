get_abs_script_path() {
pushd . >/dev/null
cd $(dirname $0)
dir=$(pwd)
popd  >/dev/null
}

get_abs_script_path
parentdir="$(dirname "$dir")"
logsFolder=$parentdir/logs
if [ ! -d "$logsFolder" ]; then
    echo "Creating logs folder"$logsFolder
    mkdir $logsFolder
fi


export CLASSPATH_PREFIX=$parentdir"/resources"


echo "Exporting system variables..."
. $parentdir/conf/jaws-env.sh

echo $TACHYON_WAREHOUSE_PATH
echo $TACHYON_MASTER
echo $MESOS_NATIVE_LIBRARY
echo $JAVA_OPTS
echo $CLASSPATH_PREFIX


echo "Deploying jaws..."
$dir/main-jaws.sh
