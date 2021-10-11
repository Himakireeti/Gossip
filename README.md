# DOSP Project 2: Gossip and Push sum

Himakireeti Konda (UFID 46432397 ) Medha Naik (UFID 28471168 )

### 	Problem Statement

Implement Gossip and Push Sum algorithms using actor model in F#, where actor nodes are connected in line, full, 3D or imperfect 3D topology.

Converge Gossip when all actors have heard rumor 10 times

Converge Push sum when all actor ratios did not change more that 10^-10 in 3 consecutive rounds



### Implementation Details

We used AKKA.net for F# to implement actor model.

We used Stopwatch from System.Diagnostics to keep track of real time. We used Process TotalProcessorTime to keep track of CPU time.



### 	How To Run Script 

dotnet fsi Gossip.fsx `number of nodes` `topology` `algorithm`

`number of nodes`  	total number of actor nodes 

`topology` 					line, full, 3D or imp3D

`algorithm`					gossip or push-sum




### 	What Works

Convergence of both Gossip and Push sum algorithms when nodes are connected in line, full, 3D and imp3D topology



### Largest Network Managed for Each Algorithm - Topology



| Algorithm | Topology | Max Nodes | CPU Time / Real Time | # Cores |
| --------- | -------- | --------- | -------------------- | ------- |
| gossip    | full     | 37000     | 4.23                 | 4       |
|           | 3D       | 2000000   | 3.87                 | 4       |
|           | imp3D    | 2000000   | 4.8                  | 5       |
|           | line     | 2000      | 3.01                 | 3       |
| push sum  | full     | 10000     | 3.98                 | 4       |
|           | line     | 1000      | 2.1                  | 2       |
|           | 3d       | 8000      | 3.45                 | 3       |
|           | imp3d    | 8000      | 3.96                 | 4       |

