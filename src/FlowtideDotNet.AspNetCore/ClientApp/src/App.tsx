import React from 'react';
import 'reactflow/dist/style.css';
import useSWR from 'swr';
import { StreamGraph, StreamGraphEdge, StreamGraphNode } from './streamgraph/streamgraph';
import { Card } from './card';
import { NodeTable } from './nodetable';

const fetcher = (url: string) => fetch(url).then(r => r.json())

function App() {
  const { data } = useSWR('@(rootpath)api/diagnostics', fetcher, { refreshInterval: 1000 })

  let graph = <div></div>
 
  const arr: Array<StreamGraphNode> = [];

  if (data) {  
    for (const property in data.nodes) {
      const d = (data.nodes as any)[property] as any
      let gauges: Array<any> = []
      if (d.gauges) {
        gauges = (d.gauges as Array<any>).map(x => {
          return {
            name: x.name,
            value: x.dimensions[""].value
          }
        })
      }
      let counters: Array<any> = []
      if (d.counters) {
        counters = d.counters.map((x: any) => {
          console.log(x.name)
          console.log(x.total.rateLastMinute)
          return {
            name: x.name,
            total: {
              rateLastMinute: x.total.rateLastMinute,
              sum: x.total.sum
            }
          }
        })
      }
      
      arr.push({
        id: property,
        displayName: d.displayName,
        gauges: gauges,
        counters: counters
      })
    }

    const edges = (data.edges as Array<any>).map<StreamGraphEdge>(x => {
      return {
        source: x.source,
        target: x.target
      };
    }) as Array<StreamGraphEdge>;

    graph = <StreamGraph nodes={arr} edges={edges} />
  }
  
  
  
  
  return (
    <div className="App">
      <div className="grid grid-cols-5 gap-4">
        <div className="col-span-1"></div>
        <div className="col-span-3">
          <Card>
            {graph}
          </Card>
          <Card>
            <NodeTable nodes={arr} />
          </Card>
        </div>
        <div className="col-span-1"></div>
      </div>
    </div>
  );
}

export default App;
