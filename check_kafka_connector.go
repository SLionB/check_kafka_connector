package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "os/exec"
    "time"
)

type KafkaTask struct {
  State string
  Trace string
  Id int
  Worker_id string
}
type KafkaConnector struct {
  State string
  Worker_id string
}
type KafkaConnectStatus struct {
  Name string
  Connector KafkaConnector
  Tasks []KafkaTask
  Type string
}

var KafkaConnectServerName = "localhost"
var KafkaConnectServerPort = "8083"
var KafkaConnectorName = "cbs-dev-app-os2200"
var KafkaConnectorServiceName = "os2200"

var restartWorkerServer = true
var pauseandresumeConnector = false
var restartConnectorTask = false
var testme = false

var KafkaConnectTestReply = `{
  "name": "cbs-dev-app-os2200",
  "connector": {
    "state": "RUNNING",
    "worker_id": "10.116.15.12:8083"
  },
  "tasks": [
  {
      "state": "FAILED",
      "trace": "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: 0308 Unable to receive the JDBC Server's response (EOF detected after 0 header bytes received); connection to the server probably lost.\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:102)\n\tat gr.unisystems.connect.jdbc.source.JdbcSourceTask.poll(JdbcSourceTask.java:225)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.poll(WorkerSourceTask.java:244)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:220)\n\tat org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:175)\n\tat org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:219)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\nCaused by: java.sql.SQLException: 0308 Unable to receive the JDBC Server's response (EOF detected after 0 header bytes received); connection to the server probably lost.\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4409)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4241)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.sendAndReceive(RdmsConnection.java:6568)\n\tat com.unisys.os2200.rdms.jdbc.RdmsStatement.execute(RdmsStatement.java:3963)\n\tat com.unisys.os2200.rdms.jdbc.RdmsWrapper_ST.execute(RdmsWrapper_ST.java:410)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.isConnectionValid(CachedConnectionProvider.java:116)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:96)\n\t... 10 more\n",
      "id": 0,
      "worker_id": "10.116.15.12:8083"
  },
  {
      "state": "FAILED",
      "trace": "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: 0415 An RDMS Thread could not be successfully established before the STMT_EXECUTE_TASK(18) was attempted.\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:102)\n\tat gr.unisystems.connect.jdbc.source.JdbcSourceTask.poll(JdbcSourceTask.java:225)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.poll(WorkerSourceTask.java:244)\n\tat org.apache.kafka.connect.runtime.WorkerSourceTask.execute(WorkerSourceTask.java:220)\n\tat org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:175)\n\tat org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:219)\n\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n\tat java.util.concurrent.FutureTask.run(FutureTask.java:266)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\nCaused by: java.sql.SQLException: 0415 An RDMS Thread could not be successfully established before the STMT_EXECUTE_TASK(18) was attempted.\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4409)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.genException(RdmsConnection.java:4241)\n\tat com.unisys.os2200.rdms.jdbc.RdmsConnection.sendAndReceive(RdmsConnection.java:5922)\n\tat com.unisys.os2200.rdms.jdbc.RdmsStatement.execute(RdmsStatement.java:3963)\n\tat com.unisys.os2200.rdms.jdbc.RdmsWrapper_ST.execute(RdmsWrapper_ST.java:410)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.isConnectionValid(CachedConnectionProvider.java:116)\n\tat gr.unisystems.connect.jdbc.util.CachedConnectionProvider.getValidConnection(CachedConnectionProvider.java:96)\n\t... 10 more\n",
      "id": 1,
      "worker_id": "10.116.15.12:8083"
  }
  ],
  "type": "source"
}`

func main() {

	shouldPauseConnector := false
	shouldRestartWorker := false
		
        kafkaConnectApiURL :=  fmt.Sprintf("http://%s:%s/connectors/%s/status", KafkaConnectServerName ,KafkaConnectServerPort, KafkaConnectorName)
	fmt.Printf("Checking Kafka Connector...%s\n", kafkaConnectApiURL)
	
	var response *http.Response
        var err error
	if (testme == true ){
		response, err = http.Get("https://httpbin.org/ip")
	} else {
		response, err = http.Get(kafkaConnectApiURL)
	}
	
		
       if err != nil {
          fmt.Printf("The HTTP request failed with error %s\n", err)
       } else {
		//data := KafkaConnectTestReply
		data, _  := ioutil.ReadAll(response.Body)
 		fmt.Println(string(data)) 
		
		var status KafkaConnectStatus	
                json.Unmarshal([]byte(data), &status)
		for i := range status.Tasks {
			//fmt.Printf("Id:%d, State: %s, WorkerID: %s\n",status.Tasks[i].Id,status.Tasks[i].State,status.Tasks[i].Worker_id)
			
			if (status.Tasks[i].State == "FAILED"){
			    shouldPauseConnector = true
				fmt.Printf("Id:%d, State: %s, WorkerID: %s\n",status.Tasks[i].Id,status.Tasks[i].State,status.Tasks[i].Worker_id)
				
				// Restarts the Tasks that FAILED
				if (restartConnectorTask == true){
					//http://localhost:8083/connectors/cbs-dev-app-os2200/tasks/0/restart
					kafkaConnectRestartTaskURL :=  fmt.Sprintf("http://%s:%s/connectors/%s/tasks/%d/restart", KafkaConnectServerName ,KafkaConnectServerPort, KafkaConnectorName, status.Tasks[i].Id)
					fmt.Printf("Restarting Kafka Task...%s\n", kafkaConnectRestartTaskURL)
				
					jsonData := ""
					jsonValue, _ := json.Marshal(jsonData)
					response, err = http.Post(kafkaConnectRestartTaskURL, "application/json", bytes.NewBuffer(jsonValue))
					if err != nil {
						fmt.Printf("The HTTP request failed with error %s\n", err)
					} else {
						data, _ := ioutil.ReadAll(response.Body)
						fmt.Println(string(data))
					}
				}
				
				if (status.Connector.Worker_id == status.Tasks[i].Worker_id ) {
					shouldRestartWorker = true
				}
			}
		}
    }
	
	// Pauses and Resumes the connector
	if (shouldPauseConnector == true && pauseandresumeConnector == true) {
	
	       //http://localhost:8083/connectors/cbs-dev-app-os2200/pause
		kafkaConnectPauseURL :=  fmt.Sprintf("http://%s:%s/connectors/%s/pause", KafkaConnectServerName ,KafkaConnectServerPort, KafkaConnectorName)
		fmt.Printf("Pausing Kafka Connect...%s\n", kafkaConnectPauseURL)

		jsonData := ""
		jsonValue, _ := json.Marshal(jsonData)
		client := http.Client{}
		client.Timeout = time.Second * 15
		req, err := http.NewRequest(http.MethodPut, kafkaConnectPauseURL, bytes.NewBuffer(jsonValue))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		response, err = client.Do(req)
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(response.Body)
			fmt.Println(string(data))
		}

		time.Sleep(time.Second * 20)

		//http://localhost:8083/connectors/cbs-dev-app-os2200/resume
		kafkaConnectResumeURL :=  fmt.Sprintf("http://%s:%s/connectors/%s/resume", KafkaConnectServerName ,KafkaConnectServerPort, KafkaConnectorName)
		fmt.Printf("Resuming Kafka Connect...%s\n", kafkaConnectResumeURL)

		req, err = http.NewRequest(http.MethodPut, kafkaConnectResumeURL, bytes.NewBuffer(jsonValue))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		response, err = client.Do(req)
		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			data, _ := ioutil.ReadAll(response.Body)
			fmt.Println(string(data))
		}
	
	}
	
	//Restarts the local worker
	if (shouldRestartWorker == true && restartWorkerServer == true) {
		fmt.Printf("Restarting Kafka Connect Service...%s\n", KafkaConnectorServiceName)
		output, err := exec.Command("systemctl", "restart", KafkaConnectorServiceName+".service").Output()
		if err != nil {
			panic(err)
		}
		fmt.Println(string(output))
	}
	
	
    fmt.Println("Terminating the application...")
}
