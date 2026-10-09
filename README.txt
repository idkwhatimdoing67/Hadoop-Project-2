# HDFS Setup (Run these once)
hdfs dfs -mkdir -p /hw2/temp /hw2/capitals
hdfs dfs -put city_temperature.csv /hw2/temp/
hdfs dfs -put state-capitals.csv /hw2/capitals/

# Q1: Compile, Package, and Run
mkdir -p classes
javac -classpath $(hadoop classpath) -d classes Q1.java
jar -cfe Q1.jar Q1 -C classes/ .
hadoop jar Q1.jar /hw2/temp /hw2/q1_output

# Q2: Compile, Package, and Run
javac -classpath $(hadoop classpath) -d classes Q2.java
jar -cfe Q2.jar Q2 -C classes/ .
hadoop jar Q2.jar /hw2/temp /hw2/capitals/state-capitals.csv /hw2/q2_output

# Q3: Compile, Package, and Run
javac -classpath $(hadoop classpath) -d classes Q3.java
jar -cfe Q3.jar Q3 -C classes/ .
hadoop jar Q3.jar /hw2/temp /hw2/capitals /hw2/q3_output