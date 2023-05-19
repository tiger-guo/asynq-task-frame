package main

import (
	"log"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func main() {

	cnf, err := config.NewFromYaml("/Users/guohu/Code/go/src/learn/src/asynq-task-frame/machinery/config.yaml", false)
	if err != nil {
		log.Println("config failed", err)
		return
	}

	server, err := machinery.NewServer(cnf)
	if err != nil {
		log.Println("start server failed", err)
		return
	}

	// 注册任务
	err = server.RegisterTask("sum", Sum)
	if err != nil {
		log.Println("reg task failed", err)
		return
	}

	worker := server.NewWorker("asong", 1)
	go func() {
		err = worker.Launch()
		if err != nil {
			log.Println("start worker error", err)
			return
		}
	}()

	// task signature
	signature := &tasks.Signature{
		Name: "sum",
		Args: []tasks.Arg{
			{
				Type:  "[]int64",
				Value: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
	}

	asyncResult, err := server.SendTask(signature)
	if err != nil {
		log.Fatal(err)
	}
	res, err := asyncResult.Get(1)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("get res is %v\n", tasks.HumanReadableResults(res))

}

func Sum(args []int64) (int64, error) {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}

	return sum, nil
}
