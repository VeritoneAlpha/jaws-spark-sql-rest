export TACHYON_WAREHOUSE_PATH=/sharktables
export TACHYON_MASTER=tachyon://devbox.local:19998
export MESOS_NATIVE_LIBRARY=/home/user/mesos-0.19.0/lib/libmesos.so
export JAVA_OPTS="$JAVA_OPTS  -Djava.library.path=/home/user/mesos-0.19.0/lib/libmesos.so:/home/user/hadoopNativeLibs"

