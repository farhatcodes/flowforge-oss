package workflow

import (
 "errors"
 "fmt"
 "sort"
)
type Task struct {
 ID string `json:"id"`
 DependsOn []string `json:"depends_on"`
}
type Graph struct {Tasks []Task `json:"tasks"`}
func(g Graph)Order()([]string,error){
 nodes:=map[string]Task{}
 degree:=map[string]int{}
 children:=map[string][]string{}
 for _,t:=range g.Tasks{
  if t.ID=="" {return nil,errors.New("empty task id")}
  if _,exists:=nodes[t.ID];exists {return nil,fmt.Errorf("duplicate task: %s",t.ID)}
  nodes[t.ID]=t
  degree[t.ID]=0
 }
 for _,t:=range g.Tasks{
  seen:=map[string]bool{}
  for _,dep:=range t.DependsOn{
   if _,exists:=nodes[dep];!exists {return nil,fmt.Errorf("unknown dependency: %s",dep)}
   if seen[dep] {return nil,fmt.Errorf("duplicate dependency: %s",dep)}
   seen[dep]=true
   degree[t.ID]++
   children[dep]=append(children[dep],t.ID)
  }
 }
 ready:=[]string{}
 for id,count:=range degree{if count==0 {ready=append(ready,id)}}
 sort.Strings(ready)
 order:=[]string{}
 for len(ready)>0{
  id:=ready[0]
  ready=ready[1:]
  order=append(order,id)
  for _,child:=range children[id]{
   degree[child]--
   if degree[child]==0 {ready=append(ready,child)}
  }
  sort.Strings(ready)
 }
 if len(order)!=len(nodes){return nil,errors.New("workflow contains a cycle")}
 return order,nil
}
