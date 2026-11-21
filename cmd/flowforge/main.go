package main

import (
    "encoding/json"
    "fmt"
    "log"
    "github.com/farhatcodes/flowforge-oss/internal/workflow"
)

func main() {
    graph := workflow.Graph{Tasks: []workflow.Task{{ID:"validate"},{ID:"reserve",DependsOn:[]string{"validate"}},{ID:"notify",DependsOn:[]string{"reserve"}}}}
    order, err := graph.Order()
    if err != nil { log.Fatal(err) }
    b,_ := json.MarshalIndent(order,"","  ")
    fmt.Println(string(b))
}
