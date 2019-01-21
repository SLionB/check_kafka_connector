package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type kafkaTask struct {
	State    string
	Trace    string
	ID       int
	WorkerID string `json:"Worker_id"`
}
type kafkaConnector struct {
	State    string
	WorkerID string `json:"Worker_id"`
}
type kafkaConnectStatus struct {
	Name      string
	Connector kafkaConnector
	Tasks     []kafkaTask
	Type      string
}

var kafkaConnectCheckTestReply = `{
  "name": "connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "127.0.0.1:8083"
  },
  "tasks": [
  {
      "state": "FAILED",
      "trace": "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: 0308 Unable to receive the JDBC Server's response (EOF detected after 0 header bytes received); connection to the server probably lost.\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:102)\n\tat gr.unisystems.connect.jdbc.source.JdbcSourceTask.poll(JdbcSourceTask.java:225)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.poll(WorkerSourceTask.java:244)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:220)\n\tat org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:175)\n\tat org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:219)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\nCaused by: java.sql.SQLException: 0308 Unable to receive the JDBC Server's response (EOF detected after 0 header bytes received); connection to the server probably lost.\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4409)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4241)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.sendAndReceive(RdmsConnection.java:6568)\n\tat com.unisys.os2200.rdms.jdbc.RdmsStatement.execute(RdmsStatement.java:3963)\n\tat com.unisys.os2200.rdms.jdbc.RdmsWrapper_ST.execute(RdmsWrapper_ST.java:410)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.isConnectionValid(CachedConnectionProvider.java:116)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:96)\n\t... 10 more\n",
      "id": 0,
      "worker_id": "127.0.0.1:8083"
  },
  {
      "state": "FAILED",
      "trace": "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: 0415 An RDMS Thread could not be successfully established before the STMT_EXECUTE_TASK(18) was attempted.\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:102)\n\tat gr.unisystems.connect.jdbc.source.JdbcSourceTask.poll(JdbcSourceTask.java:225)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.poll(WorkerSourceTask.java:244)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:220)\n\tat org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:175)\n\tat org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:219)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\nCaused by: java.sql.SQLException: 0415 An RDMS Thread could not be successfully established before the STMT_EXECUTE_TASK(18) was attempted.\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4409)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4241)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.sendAndReceive(RdmsConnection.java:5922)\n\tat com.unisys.os2200.rdms.jdbc.RdmsStatement.execute(RdmsStatement.java:3963)\n\tat com.unisys.os2200.rdms.jdbc.RdmsWrapper_ST.execute(RdmsWrapper_ST.java:410)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.isConnectionValid(CachedConnectionProvider.java:116)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:96)\n\t... 10 more\n",
      "id": 1,
      "worker_id": "127.0.0.1:8083"
  }
  ],
  "type": "source"
}`

func logline(logentry string) {
	f, err := os.OpenFile("check_kafka_connector.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	log.Println(logentry)
}

func main() {

	fmt.Println("Starting the application...")
	fmt.Println("Usage: go run check_kafka_connector.go -s=localhost:8083 -c=connector -t=false")
	fmt.Println("-s = Kafka Connect Server IP:Port(localhost:8083)")
	fmt.Println("-c = Kafka Connector Name(connector)")
	fmt.Println("-t = Kafka Connector Check Test for debbuging ONLY(false)")
	//Get commandline flags
	var kafkaConnectServer string
	var kafkaConnectorName string
	var kafkaConnectorCheckTest bool
	flag.StringVar(&kafkaConnectServer, "s", "localhost:8083", "kafkaConnectServer")
	flag.StringVar(&kafkaConnectorName, "c", "connector", "kafkaConnectorName")
	flag.BoolVar(&kafkaConnectorCheckTest, "t", false, "kafkaConnectorCheckTest")
	flag.Parse()
	fmt.Println("kafkaConnectServer has value ", kafkaConnectServer)
	fmt.Println("kafkaConnectorName has value ", kafkaConnectorName)
	fmt.Println("kafkaConnectorCheckTest has value ", kafkaConnectorCheckTest)

	//Check Connector Status with a GET request
	kafkaConnectStatusURL := fmt.Sprintf("http://%s/connectors/%s/status", kafkaConnectServer, kafkaConnectorName)
	if kafkaConnectorCheckTest == true {
		kafkaConnectStatusURL = "https://httpbin.org/ip"
	}
	fmt.Printf("Checking Kafka Connector...%s\n", kafkaConnectStatusURL)
	var netClient = &http.Client{
		Timeout: time.Second * 10,
	}
	response, err := netClient.Get(kafkaConnectStatusURL)

	//Get JSON response
	if err != nil {
		fmt.Printf("The HTTP request failed with error %s\n", err)
	} else {
		data, _ := ioutil.ReadAll(response.Body)
		if kafkaConnectorCheckTest == true {
			data = []byte(kafkaConnectCheckTestReply)
		}
		fmt.Println(string(data))

		//Unmarshal JSON response to data structure
		var status kafkaConnectStatus
		json.Unmarshal([]byte(data), &status)

		//Check for failed tasks
		for i := range status.Tasks {
			if status.Tasks[i].State == "FAILED" {
				fmt.Printf("Id:%d, State: %s, WorkerID: %s\n", status.Tasks[i].ID, status.Tasks[i].State, status.Tasks[i].WorkerID)

				// Restart failed tasks
				kafkaConnectRestartTaskURL := fmt.Sprintf("http://%s/connectors/%s/tasks/%d/restart", kafkaConnectServer, kafkaConnectorName, status.Tasks[i].ID)
				if kafkaConnectorCheckTest == true {
					kafkaConnectRestartTaskURL = "https://httpbin.org/post"
				}
				fmt.Printf("Restarting Kafka Task...%s\n", kafkaConnectRestartTaskURL)
				jsonData := ""
				jsonValue, _ := json.Marshal(jsonData)
				var netClient = &http.Client{
					Timeout: time.Second * 10,
				}
				response, err := netClient.Post(kafkaConnectRestartTaskURL, "application/json", bytes.NewBuffer(jsonValue))
				if err != nil {
					fmt.Printf("The HTTP request failed with error %s\n", err)
				} else {
					data, _ := ioutil.ReadAll(response.Body)
					fmt.Println(string(data))
					logline(fmt.Sprintf("Restarting Kafka Task with Id:%d on WorkerID: %s for Connector: %s", status.Tasks[i].ID, status.Tasks[i].WorkerID, status.Name))
				}
			} // if status.Tasks[i].State == "FAILED"
		} // for i := range status.Tasks
	} // else

	fmt.Println("Terminating the application...")
}
