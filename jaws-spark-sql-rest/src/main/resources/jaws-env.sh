export TACHYON_WAREHOUSE_PATH=/sharktables
export TACHYON_MASTER=tachyon://devbox.local:19998
export MESOS_NATIVE_LIBRARY=/home/user/mesos-0.19.0/lib/libmesos.so
export LOGGING_OPTS="-Dlog4j.configuration=log4j.properties -DJAWS_LOG_FOLDER=$1"
export JAVA_OPTS="$LOGGING_OPTS -XX:PermSize=1g -XX:MaxPermSize=1g -Djava.library.path=/home/user/mesos-0.19.0/lib/libmesos.so:/home/user/hadoopNativeLibs"

