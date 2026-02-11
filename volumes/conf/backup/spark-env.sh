JAVA_HOME_PATH=$(dirname "$(dirname "$(readlink -f "$(which java)")")")
echo "Detected JAVA_HOME: $JAVA_HOME_PATH"

sudo tee $SPARK_HOME/conf/spark-env.sh >/dev/null <<EOF
export JAVA_HOME=${JAVA_HOME_PATH}
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
EOF

sudo chmod +x $SPARK_HOME/conf/spark-env.sh
cat $SPARK_HOME/conf/spark-env.sh
