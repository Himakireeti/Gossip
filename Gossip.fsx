#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 


open System
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic

// message types
type Message =
    |Connect of IActorRef[] 
    |SetValues of int * IActorRef[] * int
    |StartGossip of String
    |EndGossip of String
    |PushSum of float * float
    |StartPushSum of int
    |EndPushSum of float * float

// helper globals
let random  = System.Random()
let timer = Stopwatch()
let proc = Process.GetCurrentProcess()
let cpuTimestamp = proc.TotalProcessorTime
let system = ActorSystem.Create("System")
let mutable TimeToConverge=0


// helper actor to keep track of time and ending the program on convergence
let Helper (mailbox:Actor<_>) = 

    let mutable nodes:IActorRef[] = [||]
    let mutable msgCount = 0
    let mutable nodeCount = 0
    let mutable startTime = 0
    let mutable totalNodes =0
    

    let rec loop() = actor {

            let! message = mailbox.Receive()
            match message with 

            | EndGossip message ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                msgCount <- msgCount + 1

                if msgCount = totalNodes then
                    let realTime = timer.ElapsedMilliseconds
                    let cpuTime = (proc.TotalProcessorTime - cpuTimestamp).TotalMilliseconds
                    printfn "CPU time = %dms" (int64 cpuTime)
                    printfn "Timer Convergence : %A ms" realTime
                    TimeToConverge <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart= random.Next(0,nodes.Length)
                    nodes.[newStart] <! StartGossip("Hi")

            | EndPushSum (s,w) ->
                let endTime = System.DateTime.Now.TimeOfDay.Milliseconds
                nodeCount <- nodeCount + 1

                if nodeCount = totalNodes then
                    let realTime = timer.ElapsedMilliseconds
                    let cpuTime = (proc.TotalProcessorTime - cpuTimestamp).TotalMilliseconds
                    printfn "CPU time = %dms" (int64 cpuTime)
                    printfn "Timer Convergence : %A ms" realTime
                    TimeToConverge <-endTime-startTime
                    Environment.Exit 0

                else
                    let newStart=random.Next(0,nodes.Length)
                    nodes.[newStart] <! PushSum(s,w)
          
            | SetValues (strtTime,nodesRef,totNds) ->
                startTime <-strtTime
                nodes <- nodesRef
                totalNodes <-totNds
                
            | _->()

            return! loop()
        }
    loop()

    

// actor node communicating in gossip or push-sum
let Node helper nodeIndex (mailbox:Actor<_>)  =

    let mutable heardMessageCount = 0 
    let mutable neighbors:IActorRef[]=[||]

    // push sum starter values
    let mutable s = nodeIndex |> float
    let mutable w = 1.0
    let mutable r1 = 0.0
    let mutable r2 = 0.0
    let mutable r3 = 0.0
    let mutable r4 = 0.0
    let threshold = 10.0**(-10.0)
    
    let mutable termRound = 1.0
    let mutable flag = 0
    let mutable counter = 1
    let mutable converged = 0
    
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with 

        // add neighbours to the node
        | Connect nArray->
                neighbors<-nArray

        // push sum protocol messages
        | StartPushSum ind->
            let index = random.Next(0,neighbors.Length)
            let sn = index |> float
            neighbors.[index] <! PushSum(sn,1.0)

        | PushSum (sum,weight)->
            // do calculation until convergence
            if(converged = 1) then
                let index = random.Next(0,neighbors.Length)
                neighbors.[index] <! PushSum(sum,weight)
            
            if(flag = 0) then
                match counter with
                | 1 ->
                    r1<-s/w
                | 2 ->
                    r2<-s/w
                | 3 ->
                    r3<-s/w
                    flag<-1
                | _ -> 

                counter<-counter+1

            s<-s+sum
            s<-s/2.0
            w<-w+weight
            w<-w/2.0
            
            r4<-s/w
           
            if(flag=0) then
                let index= random.Next(0,neighbors.Length)
                neighbors.[index] <! PushSum(s,w)                

            
            if(abs(r1-r4)<=threshold && converged=0) then 
                converged<-1
                helper <! EndPushSum(s,w)

            else
                r1<-r2
                r2<-r3
                r3<-r4
                let index= random.Next(0,neighbors.Length)
                neighbors.[index] <! PushSum(s,w)

        
        // gossip protocol messages
        | StartGossip msg ->

                heardMessageCount<- heardMessageCount+1

                if(heardMessageCount = 10) then
                      helper <! EndGossip(msg)

                else
                      let index= random.Next(0,neighbors.Length)
                      neighbors.[index] <! StartGossip(msg)


        | _-> ()

        return! loop()
    }
    loop()


// communicate in given protocol
module protocol=

        // start given protocol
        let call algorithm nodeCount nodes=
            (nodes : _ array)|>ignore
            match algorithm with
            | "gossip" ->
                let first= random.Next(0,nodeCount-1)
                nodes.[first]<!StartGossip("Hi")
            | "push-sum" ->
                let first= random.Next(0,nodeCount-1)
                nodes.[first]<!StartPushSum(first)
            | _ ->
                printfn "Invalid protocol - try gossip or push-sum"
                Environment.Exit 0

            
// communicate in given topology
module topology=
        
        // communicate in full topology
        let full nodeCount algorithm=
            let helper= 
                Helper
                    |> spawn system "helper"

            let nodes = Array.zeroCreate(nodeCount)

            let mutable neighbors:IActorRef[]=Array.empty
            
            // create actor nodes
            for i in [0..nodeCount-1] do
                nodes.[i]<- Node helper (i+1)
                                    |> spawn system ("Node"+string(i))
            
            // connect neighbors in full topology
            for i in [0..nodeCount-1] do
                if i=0 then
                    neighbors<-nodes.[1..nodeCount-1]
                    nodes.[i]<!Connect(neighbors)

                elif i=(nodeCount-1) then 
                    neighbors<-nodes.[0..(nodeCount-2)]
                    nodes.[i]<!Connect(neighbors)

                else
                    neighbors<-Array.append nodes.[0..i-1] nodes.[i+1..nodeCount-1]
                    nodes.[i]<!Connect(neighbors)
           
            // start timer
            timer.Start()       
            helper<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodes,nodeCount)

            // start communicating
            protocol.call algorithm nodeCount nodes

        // communicate in perfect 3D topology
        let p3D nodeCount algorithm=
            let helper= 
                Helper
                    |> spawn system "helper"
            
            let layers=int(ceil ((float nodeCount)** 0.33))
            let layerNodeCount=int(layers*layers)

            let nodes = Array.zeroCreate(layerNodeCount*layers)
            
            // create actor nodes
            for i in [0..int(layerNodeCount*layers)-1] do
                nodes.[i]<- Node helper (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable cycle = ((((nodeCount |> float) ** 0.33) |> ceil) ** 2.0) |> int
            let mutable step = 0

            // connect neighbors in perfect 3D topology
            for layer in [1..layers] do
                for i in [0..layerNodeCount-1] do
                    // printfn "%d %d" layer (step+i) 
                    let mutable neighbors:IActorRef[]=Array.empty
                    if i=0 then
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i+layers..step+i+layers]

                    elif i=layers-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+layers..step+i+layers]
                    
                    elif i=layerNodeCount-layers then 
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i-layers..step+i-layers]

                    elif i=layerNodeCount-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i-layers..step+i-layers]

                    elif i<layers-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 

                    elif i>layerNodeCount-layers && i<layerNodeCount-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i-layers..step+i-layers] 

                    elif i%layers = 0 then 
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 

                    elif (i+1)%layers = 0 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 
                   
                    else
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 

                    if layer =0 then
                        neighbors <- Array.append neighbors nodes.[i+layerNodeCount..i+layerNodeCount]
                    elif layer=cycle-1 then
                        neighbors <- Array.append neighbors nodes.[step+i-layerNodeCount..step+i-layerNodeCount]
                    else
                        neighbors <- Array.append neighbors nodes.[step+i-layerNodeCount..step+i-layerNodeCount]
                        neighbors <- Array.append neighbors nodes.[step+i+layerNodeCount..step+i+layerNodeCount]

                    nodes.[step+i]<!Connect(neighbors)

                step <- step+cycle

            // start timer
            timer.Start()
            helper<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodes,layerNodeCount)

            // start communicating
            protocol.call algorithm nodeCount nodes


        // communicate in line topology
        let line nodeCount algorithm =

            let helper= 
                Helper
                    |> spawn system "helper"

            let nodes = Array.zeroCreate(nodeCount)
            
            // create actor nodes
            for i in [0..nodeCount-1] do
                nodes.[i]<- Node helper (i+1)
                                    |> spawn system ("Node"+string(i))
            
            let mutable neighbors:IActorRef[]=Array.empty

            // connect neighbors in line topology
            for i in [0..nodeCount-1] do
                if i=0 then
                    neighbors<-nodes.[1..1]    
                    nodes.[i]<!Connect(neighbors)

                elif i=(nodeCount-1) then 
                    neighbors<-nodes.[(nodeCount-2)..(nodeCount-2)]                   
                    nodes.[i]<!Connect(neighbors)

                else
                    neighbors<-Array.append nodes.[i-1..i-1] nodes.[i+1..i+1]            
                    nodes.[i]<!Connect(neighbors)
            
            // start timer
            timer.Start()
            helper<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodes,nodeCount)

            // start communicating
            protocol.call algorithm nodeCount nodes
        

        // communicate in imperefect 3D topology
        let imp3D nodeCount algorithm=

            let helper = 
                Helper
                    |> spawn system "helper"
            
            let layers=int(ceil ((float nodeCount)** 0.33))
            let layerNodeCount=int(layers*layers)

            let nodes = Array.zeroCreate(layerNodeCount*layers)
            
            // create actor nodes
            for i in [0..int(layerNodeCount*layers)-1] do
                nodes.[i]<- Node helper (i+1)
                                    |> spawn system ("Node"+string(i))

            let mutable cycle = ((((nodeCount |> float) ** 0.33) |> ceil) ** 2.0) |> int
            let mutable step = 0

            // connect neighbors in imperfect 3D topology
            for layer in [1..layers] do
                for i in [0..layerNodeCount-1] do
                    let mutable neighbors:IActorRef[]=Array.empty
                    if i=0 then
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i+layers..step+i+layers]

                    elif i=layers-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+layers..step+i+layers]

                    elif i=layerNodeCount-layers then 
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i-layers..step+i-layers]

                    elif i=layerNodeCount-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i-layers..step+i-layers]

                    elif i<layers-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 
                        
                   

                    elif i>layerNodeCount-layers && i<layerNodeCount-1 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i-layers..step+i-layers] 

                    elif i%layers = 0 then 
                        neighbors<-Array.append nodes.[step+i+1..step+i+1] nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 

                    elif (i+1)%layers = 0 then 
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 
                   
                    else
                        neighbors<-Array.append nodes.[step+i-1..step+i-1] nodes.[step+i+1..step+i+1] 
                        neighbors<-Array.append neighbors nodes.[step+i-layers..step+i-layers] 
                        neighbors<-Array.append neighbors nodes.[step+i+layers..step+i+layers] 

                    if layer =0 then
                        neighbors <- Array.append neighbors nodes.[i+layerNodeCount..i+layerNodeCount]
                        neighbors <- Array.append neighbors nodes.[i+layerNodeCount+1..i+layerNodeCount+1]

                    elif layer=cycle-1 then
                        neighbors <- Array.append neighbors nodes.[step+i-layerNodeCount..step+i-layerNodeCount]
                        neighbors <- Array.append neighbors nodes.[step+i-layerNodeCount-1..step+i-layerNodeCount-1]

                    else
                        neighbors <- Array.append neighbors nodes.[step+i-layerNodeCount..step+i-layerNodeCount]
                        neighbors <- Array.append neighbors nodes.[step+i+layerNodeCount..step+i+layerNodeCount]
                        neighbors <- Array.append neighbors nodes.[step+i+layerNodeCount-1..step+i+layerNodeCount-1]


                    
                    nodes.[step+i]<!Connect(neighbors)

                step <- step+cycle

            // start timer
            timer.Start()
            helper<!SetValues(System.DateTime.Now.TimeOfDay.Milliseconds,nodes,layerNodeCount)

            // start communicating
            protocol.call algorithm nodeCount nodes


        let communicate nodeCount topology algorithm=
            match topology with
            | "full" ->
                full nodeCount algorithm
            | "3D" ->
                p3D nodeCount algorithm
            | "line" ->
                line nodeCount algorithm
            | "imp3D" ->
                imp3D nodeCount algorithm
            | _ -> 
                printfn "Invalid topology - try full, 3D, line or imp3D"
                Environment.Exit 0

// Execution starts here            
module mainModule=

        let args : string array = fsi.CommandLineArgs |> Array.tail

        printfn "Node count: %s, Topology: %s, Algorithm: %s" args.[0] args.[1] args.[2]

        // communicate nodeCount topology_name algorithm
        topology.communicate (args.[0] |> int) args.[1] args.[2]
   
        System.Console.ReadLine() |> ignore