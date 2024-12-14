# Sample CLI

To compile:

```shell
javac -d out -cp "lib/*" $(find src/ -name "*.java")
```

## HyDFS

HyDFS server initialization

```shell
java -cp "out:lib/*" main.java.hydfs.Main 1 127.0.0.1 8081 9091 5
java -cp "out:lib/*" main.java.hydfs.Main 2 127.0.0.1 8082 9092 5
java -cp "out:lib/*" main.java.hydfs.Main 3 127.0.0.1 8083 9093 5
java -cp "out:lib/*" main.java.hydfs.Main 4 127.0.0.1 8084 9094 5
java -cp "out:lib/*" main.java.hydfs.Main 5 127.0.0.1 8085 9095 5
```

Join with server node ID = 1 as the introducer:

```
join 127.0.0.1:8081
```

Create a HyDFS file business_1.txt from local file:

```
create testData/HyDFS/business/business_1.txt business_1.txt
```

Get HyDFS file business_1.txt and store it to local as b1.txt:

```
get business_1.txt b1.txt
```

Append content of local file business_2.txt to an existing HyDFS file business_1.txt:

```
append testData/HyDFS/business/business_2.txt business_1.txt
```

## Rain Storm

Leader and worker initialization:

```shell
java -cp "out:lib/*" main.java.rainStorm.Leader 1 127.0.0.1 8081 9091 5 7071
java -cp "out:lib/*" main.java.rainStorm.Worker 2 127.0.0.1 8082 9092 5 7072
java -cp "out:lib/*" main.java.rainStorm.Worker 3 127.0.0.1 8083 9093 5 7073
java -cp "out:lib/*" main.java.rainStorm.Worker 4 127.0.0.1 8084 9094 5 7074
java -cp "out:lib/*" main.java.rainStorm.Worker 5 127.0.0.1 8085 9095 5 7075
java -cp "out:lib/*" main.java.rainStorm.Worker 6 127.0.0.1 8086 9096 5 7076
java -cp "out:lib/*" main.java.rainStorm.Worker 7 127.0.0.1 8087 9097 5 7077
```

Command to join the leader:

```
join 127.0.0.1:8081 7071
```

Command to create new files:

```
create testData/RainStorm/TrafficSigns_15.csv ts_15.csv
create testData/RainStorm/TrafficSigns_100.csv ts_100.csv
create testData/RainStorm/TrafficSigns_1000.csv ts_1000.csv
create testData/RainStorm/TrafficSigns_5000.csv ts_5000.csv
create testData/RainStorm/TrafficSigns_10000.csv ts_10000.csv
```

Initiate a Rain Storm application with ts_1000.csv as the source file and ts_1000_app1.csv as the destination file:

```
RainStorm ts_1000.csv ts_1000_app1.csv 3
```

Set operations for an application

- Task1:

  ```
  java bolts/App1Op1 8 "Streetname"
  false
  java bolts/App1Op2 2 3
  false
  ```

- Task2:
  ```
  java bolts/App2Op1 "Punched Telespar"
  false
  java bolts/App2Op2
  true
  ```

