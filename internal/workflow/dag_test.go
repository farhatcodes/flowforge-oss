package workflow
import "testing"
func TestOrder(t *testing.T){
 g:=Graph{Tasks:[]Task{{ID:"ship",DependsOn:[]string{"pay"}},{ID:"pay"},{ID:"notify",DependsOn:[]string{"ship"}}}}
 got,err:=g.Order()
 if err!=nil||len(got)!=3||got[0]!="pay"||got[1]!="ship"||got[2]!="notify" {t.Fatalf("got=%v err=%v",got,err)}
}
func TestCycle(t *testing.T){
 _,err:=(Graph{Tasks:[]Task{{ID:"a",DependsOn:[]string{"b"}},{ID:"b",DependsOn:[]string{"a"}}}}).Order()
 if err==nil {t.Fatal("expected cycle error")}
}
func TestUnknownDependency(t *testing.T){
 _,err:=(Graph{Tasks:[]Task{{ID:"a",DependsOn:[]string{"missing"}}}}).Order()
 if err==nil {t.Fatal("expected dependency error")}
}
